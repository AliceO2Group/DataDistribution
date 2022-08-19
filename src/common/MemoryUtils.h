// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.


#ifndef DATADIST_MEMORY_UTILS_H_
#define DATADIST_MEMORY_UTILS_H_

#include <boost/container/small_vector.hpp>
#include <boost/filesystem.hpp>
#include <boost/icl/interval_map.hpp>
#include <boost/icl/right_open_interval.hpp>

#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQChannel.h>

#include <Headers/DataHeader.h>

#include "DataDistLogger.h"

#include <vector>
#include <mutex>
#include <memory>
#include <thread>
#include <chrono>

#include <sys/mman.h>
#include <cstdlib>
#include <unistd.h>

#if defined(__linux__)
#include <sys/resource.h>
#endif

namespace icl = boost::icl;

class DataHeader;
#include <fairmq/FwdDecls.h>

namespace o2::DataDistribution
{

static constexpr const char *ENV_NOLOCK = "DATADIST_NO_MLOCK";
static constexpr const char *ENV_SHM_PATH = "DATADIST_SHM_PATH";
static constexpr const char *ENV_SHM_DELAY = "DATADIST_SHM_DELAY";
static constexpr const char *ENV_SHM_ZERO = "DATADIST_SHM_ZERO";
static constexpr const char *ENV_SHM_ZERO_CHECK = "DATADIST_SHM_ZERO_CHECK";

enum RegionAllocStrategy {
  eFindLongest,
  eFindFirst
};

enum RegionATrackingStrategy {
  eExactRegion,
  eRefCount
};

template<size_t ALIGN = 64,
         RegionAllocStrategy ALLOC_STRATEGY = eFindLongest,
         RegionATrackingStrategy FREE_STRATEGY = eExactRegion
         >
class RegionAllocatorResource
{

public:
  RegionAllocatorResource() = delete;

  RegionAllocatorResource(const std::string &pSegmentName, const std::optional<uint16_t> pSegmentId, std::size_t pSize,
                          fair::mq::TransportFactory& pShmTrans,
                          std::uint64_t pRegionFlags = 0,
                          bool pCanFail = false)
  : mSegmentName(pSegmentName),
    mCanFail(pCanFail),
    mTransport(pShmTrans)
  {
    static_assert(ALIGN && !(ALIGN & (ALIGN - 1)), "Alignment must be power of 2");

    fair::mq::RegionConfig lRegionCfg;

    pSize = align_size_up(pSize);

    int lMapFlags = 0;
    std::string lSegmentRoot = "";

    // don't reserve swap space
#if defined(MAP_NORESERVE)
    lMapFlags |= MAP_NORESERVE;
#endif

    // and try to lock the memory
#if defined(MAP_LOCKED) && defined(__linux__)
{
    struct rlimit lMyLimits;
    getrlimit(RLIMIT_MEMLOCK, &lMyLimits);

    if (lMyLimits.rlim_cur >= pSize) {
      lMapFlags |= MAP_LOCKED;
    } else {
      lRegionCfg.lock = false;
      if (std::getenv(ENV_NOLOCK)) {
        WDDLOG("MemoryResource: Memory locking disabled via {} env variable. Not suitable for production.",
          ENV_NOLOCK);
      } else {
        EDDLOG("MemoryResource: Failed to lock the memory region. Increase your memory lock limits (ulimit -l).");
        EDDLOG("MemoryResource: To run without memory locking define {} env variable. Not suitable for production.",
          ENV_NOLOCK);
        throw std::bad_alloc();
      }
    }
}
#endif

    // populate the mapping
#if defined(MAP_POPULATE)
    lMapFlags |= MAP_POPULATE;
#endif

    // try to use different file mapping (hugetlbfs)
    const auto lHugetlbfsPath = std::getenv(ENV_SHM_PATH);
    if (lHugetlbfsPath) {
      do {
        // make sure directory exists
        namespace bfs = boost::filesystem;
        bfs::path lDirPath(lHugetlbfsPath);
        if (!bfs::is_directory(lDirPath)) {
          EDDLOG("Hugetlbfs mountpoint does not exist. Not using huge pages. {}={}",
            ENV_SHM_PATH, lHugetlbfsPath);
          break;
        }

        // check if the hugetlbs is writeable
        if (0 != access(lHugetlbfsPath, W_OK)) {
          EDDLOG("Hugetlbfs mountpoint is not writeable. "
            "Make sure the permissions are properly set. {}={}", ENV_SHM_PATH, lHugetlbfsPath);
          break;
        }

        lSegmentRoot = lHugetlbfsPath;
        lSegmentRoot += bfs::path::preferred_separator;
      } while (false);
    }

    // Get the environment variable for memory zeroing on init and reclaim
    const bool lZeroShmMemory = !!std::getenv(ENV_SHM_ZERO);
    const bool lZeroCheckShmMemory = !!std::getenv(ENV_SHM_ZERO_CHECK);

    if (pSegmentId.has_value()) {
      IDDLOG("Opening an existing UnmanagedRegion name={} path={} size={} id={}", mSegmentName, lSegmentRoot, pSize, pSegmentId.value());
      lRegionCfg.id = pSegmentId;
      lRegionCfg.lock = false;
      lRegionCfg.zero = lZeroShmMemory;
      lRegionCfg.removeOnDestruction = false;
    } else {
      IDDLOG("Creating new UnmanagedRegion name={} path={} size={}", mSegmentName, lSegmentRoot, pSize);
      lRegionCfg.lock = true;
      lRegionCfg.zero = lZeroShmMemory;
      lRegionCfg.removeOnDestruction = true;
    }

    auto lReclaimFn = [this, lZeroShmMemory, lZeroCheckShmMemory](const std::vector<fair::mq::RegionBlock>& pBlkVect) {
      if constexpr (FREE_STRATEGY == eExactRegion) {
        icl::interval_map<std::size_t, std::size_t> lIntMap;
        static thread_local double sMergeRatio = 0.5;

        std::uint64_t lReclaimed = 0;

        for (const auto &lInt : pBlkVect) {
          if (lInt.size == 0) {
            continue;
          }

          // align up the message size for correct interval merging
          const auto lASize = align_size_up(lInt.size);

          // check for buffer sentinel value
          if (lZeroCheckShmMemory && (lASize > lInt.size)) {
            const auto lTrailer = reinterpret_cast<const char*>(lInt.ptr)[lInt.size];
            if (lTrailer != char(0xAA)) {
              EDDLOG_RL(10000, "Memory corruption in returned message. Overwritten trailer. region={} value={}",
              mSegmentName, lTrailer);
            }
          }

          lIntMap += std::make_pair(
            icl::discrete_interval<std::size_t>::right_open(
              std::size_t(lInt.ptr), std::size_t(lInt.ptr) + lASize), std::size_t(1));
        }

        {
          // callback to be called when message buffers no longer needed by transports
          std::scoped_lock lock(mReclaimLock);

          for (const auto &lIntMerged : lIntMap) {
            if (lIntMerged.second > 1) {
              EDDLOG_GRL(1000, "UnmanagedRegion reclaim BUG! Multiple overlapping intervals:");
              EDDLOG_GRL(1000, "Intervals - [{},{}) : count={}", lIntMerged.first.lower(), lIntMerged.first.upper(), lIntMerged.second);

              // continue; // skip the overlapping thing
            }

            const std::size_t lLen = lIntMerged.first.upper() - lIntMerged.first.lower();
            lReclaimed += lLen;

            // zero and reclaim
            void *lStart = (void *) lIntMerged.first.lower();

            // clear the memory
            if (lZeroShmMemory) {
              memset(lStart, 0x00, lLen);
            }

            // recover the merged region
            reclaimSHMMessage(lStart, lLen);
          }

          mFree += lReclaimed;
          mGeneration += 1;
        }

        // weighted average merge ratio
        sMergeRatio = sMergeRatio * 0.75 + double(pBlkVect.size() - lIntMap.iterative_size()) /
          double(pBlkVect.size()) * 0.25;
        DDDLOG_RL(5000, "Memory segment '{}'::block merging ratio average={:.4}", mSegmentName, sMergeRatio);
      }
      else if constexpr (FREE_STRATEGY == eRefCount) {
        static thread_local boost::container::small_vector<fair::mq::RegionBlock, 512> sBlkVect;
        sBlkVect.clear();

        std::copy(pBlkVect.begin(), pBlkVect.end(), std::back_inserter(sBlkVect));
        std::sort(sBlkVect.begin(), sBlkVect.end(), [](auto &a, auto &b) { return a.ptr < b.ptr; } );

        {
          std::scoped_lock lRefCntLock(mAllocBlocksLock);

          // Deref lambda
          const auto lAllocDeref = [&](auto pBlockIter) {
            assert ((pBlockIter != mAllocBlocksMap.end())  && (pBlockIter->second.mRefCnt > 0));

            if (--pBlockIter->second.mRefCnt == 0) {
              void* lStart = reinterpret_cast<void*>(pBlockIter->second.mStart);
              const std::size_t lLength = pBlockIter->second.mLength;

              // clear the memory
              if (lZeroShmMemory) {
                memset(lStart, 0x00, lLength);
              }

              {
                std::scoped_lock lock(mReclaimLock);
                reclaimSHMMessage(lStart, lLength);
                mFree += lLength;
                mGeneration += 1;

              }
              mAllocBlocksMap.erase(pBlockIter);
            }
          };


          auto lLastValIter = mAllocBlocksMap.end();

          for (const auto lBlock : sBlkVect) {
            if (lBlock.size == 0) {
              continue; // no refcount for zero length messages
            }

            // check if we are in the cached refcnt interval
            if ((lLastValIter != mAllocBlocksMap.end()) && lLastValIter->second.in_range(lBlock.hint, lBlock.size)) {
              lAllocDeref(lLastValIter);
            } else {
              // find and cache the new interval
              lLastValIter = mAllocBlocksMap.lower_bound(reinterpret_cast<std::size_t>(lBlock.ptr));
              assert (!mAllocBlocksMap.empty());
              assert (lLastValIter != mAllocBlocksMap.end());
              assert (lLastValIter->second.in_range(lBlock.ptr, lBlock.size));
              lAllocDeref(lLastValIter);
            }
          }
        }
      }
    };

    mRegion = pShmTrans.CreateUnmanagedRegion(
      pSize,
      pRegionFlags,
      lReclaimFn,
      lSegmentRoot.c_str(),
      lMapFlags,
      lRegionCfg
    );

    if (!mRegion) {
      EDDLOG("Creation of memory region failed. name={} size={} path={}",
      mSegmentName, pSize, lSegmentRoot);
      throw std::bad_alloc();
    }

    mStart = static_cast<char*>(mRegion->GetData());
    mSegmentAddr = static_cast<char*>(mRegion->GetData());
    mUCXSegmentAddr = static_cast<char*>(mRegion->GetData()); // set when region is mapped
    mSegmentSize = mRegion->GetSize();
    mLength = mRegion->GetSize();
    mFree = mSegmentSize;

    // Insert delay for testing
    const auto lShmDelay = std::getenv(ENV_SHM_DELAY);
    if (lShmDelay && mSegmentName.find("O2DataRegion") != std::string::npos) {
      try {
        double lDelaySec = std::stod(lShmDelay);
        lDelaySec = std::abs(lDelaySec);

        WDDLOG("Memory segment '{}': delaying processing for specified={}s",
          mSegmentName, lDelaySec);
        std::this_thread::sleep_for(std::chrono::duration<double>(lDelaySec));
      } catch (const std::logic_error &e) {
        EDDLOG("Memory segment '{}': invalid delay specified={} error={}",
          mSegmentName, lShmDelay, e.what());
      }
    }

    // start the allocations
    mRunning = true;
  }

  ~RegionAllocatorResource() {
    // Ensure the region is destructed before anything else in this object
    mRegion.reset();
  }

  auto address() const { return mRegion->GetData(); }
  void set_ucx_address(void *ucx_address) { mUCXSegmentAddr = reinterpret_cast<char*>(ucx_address); }
  void* get_ucx_ptr(void *ptr) const  { return (reinterpret_cast<char*>(ptr) - mSegmentAddr + mUCXSegmentAddr); }

  inline
  fair::mq::MessagePtr NewFairMQMessage(const std::size_t pSize) {
    auto* lMem = do_allocate(pSize);
    if (lMem) {
      if constexpr (FREE_STRATEGY == eRefCount) {
        std::scoped_lock lLock(mAllocBlocksLock);
        auto lBlkIter = mAllocBlocksMap.lower_bound(reinterpret_cast<std::size_t>(lMem));
        assert (lBlkIter != mAllocBlocksMap.end());
        assert (lBlkIter->second.in_range(lMem, pSize));

        lBlkIter->second.mRefCnt += 1;
      }
      return mTransport.CreateMessage(mRegion, lMem, pSize);
    } else {
      return nullptr;
    }
  }

  template<typename T>
  inline
  fair::mq::MessagePtr NewFairMQMessage(const T pData, const std::size_t pSize) {
    static_assert(std::is_pointer_v<T>, "Require pointer");

    auto lMessage = NewFairMQMessage(pSize);
    if (lMessage) {
      std::memcpy(lMessage->GetData(), pData, pSize);
    }
    return lMessage;
  }

  inline
  fair::mq::MessagePtr NewFairMQMessageFromPtr(void *pPtr, std::size_t pSize) {
    // we can have a zero allocation
    if ((pSize > 0) && (pPtr != nullptr)) {
      assert(pPtr >= static_cast<char*>(mRegion->GetData()));
      assert(static_cast<char*>(pPtr)+pSize <= static_cast<char*>(mRegion->GetData()) + mRegion->GetSize() - 1);
    } else {
      // zero size: make sure the pointer is from the region or fmq will complain
      pPtr = reinterpret_cast<char*>(mRegion->GetData()) + mRegion->GetSize() - 1;
      pSize = 0;
    }

    if constexpr (FREE_STRATEGY == eRefCount) {
      if (pSize > 0) {
        std::scoped_lock lLock(mAllocBlocksLock);
        auto lBlkIter = mAllocBlocksMap.lower_bound(reinterpret_cast<std::size_t>(pPtr));
        assert (lBlkIter != mAllocBlocksMap.end());
        assert (lBlkIter->second.in_range(pPtr, pSize));

        lBlkIter->second.mRefCnt += 1;
      }
    }

    if constexpr (FREE_STRATEGY == eExactRegion) {
      if (pSize > 0) {
        assert (! (reinterpret_cast<std::size_t>(pPtr) & (ALIGN-1)));
      }
    }

#ifndef NDEBUG
    if (pSize > 0) {
      std::scoped_lock lock(mReclaimLock);

      auto interval = icl::discrete_interval<std::size_t>::right_open(
        reinterpret_cast<std::size_t>(pPtr),
        reinterpret_cast<std::size_t>(pPtr)+pSize
      );

      if (icl::intersects(mFreeRanges, interval)) {
        assert (false);
      }

      if constexpr (FREE_STRATEGY == eExactRegion) {
        auto intervalA = icl::discrete_interval<std::size_t>::right_open(
          reinterpret_cast<std::size_t>(pPtr),
          reinterpret_cast<std::size_t>(pPtr)+align_size_up(pSize)
        );

        if (icl::intersects(mFreeRanges, intervalA)) {
          assert (false);
        }
      }
    }
#endif
    return mTransport.CreateMessage(mRegion, pPtr, pSize);
  }

  template <typename OutIter>
  inline void NewFairMQMessageFromPtr(const std::vector<std::pair<void*, std::size_t>> &pAllocs, OutIter pInsertIt) {

    if constexpr (FREE_STRATEGY == eRefCount) {
      // increase refcounts
      std::scoped_lock lLock(mAllocBlocksLock);

      auto lBlkIter = mAllocBlocksMap.end();

      for (const auto &lPtrSize : pAllocs) {
        if (lPtrSize.second > 0) {
          // refcount all non-zero length messages
          // check if we need a new lookup
          if (!(lBlkIter != mAllocBlocksMap.end() && lBlkIter->second.in_range(lPtrSize.first, lPtrSize.second))) {
            lBlkIter = mAllocBlocksMap.lower_bound(reinterpret_cast<std::size_t>(lPtrSize.first));
            assert (lBlkIter != mAllocBlocksMap.end());
            assert (lBlkIter->second.in_range(lPtrSize.first, lPtrSize.second));
          }
          lBlkIter->second.mRefCnt += 1;
        }
      }
    }

    for (const auto &lPtrSize : pAllocs) {
      if (lPtrSize.second > 0) {
        *pInsertIt++ = std::move(mTransport.CreateMessage(mRegion, lPtrSize.first, lPtrSize.second));
      } else {
        // zero size: make sure the pointer is from the region or fmq will complain
        *pInsertIt++ = std::move(mTransport.CreateMessage(mRegion,
          reinterpret_cast<char*>(mRegion->GetData()) + mRegion->GetSize() - 1, 0));
      }
    }
  }

  void stop() {
    std::scoped_lock lock(mReclaimLock);
    mRunning = false;
  }

  std::size_t free() const { return mFree; }
  std::size_t size() const { return mSegmentSize; }

  bool running() const { return mRunning; }

  // NOTE: we align sizes of returned messages, but keep the exact size for allocation
  //       otherwise the shm messages would be larger than requested
  static constexpr inline
  std::size_t align_size_up(const std::size_t pSize) {
    return (pSize + ALIGN - 1) / ALIGN * ALIGN;
  }

  void* do_allocate(const std::size_t pSize)
  {
    if (!mRunning) {
      return nullptr;
    }

    if (pSize == 0) {
      // return last address of the segment
      return reinterpret_cast<char*>(mRegion->GetData()) + mRegion->GetSize() - 1;
    }

    // align up
    const auto pSizeUp = align_size_up(pSize);

    auto lRet = try_alloc(pSizeUp);

    while (!lRet && mRunning) {
      auto lGen = mGeneration.load();
      // try to reclaim if possible
      if (try_reclaim(pSizeUp)) {
        // try again
        lRet = try_alloc(pSizeUp);
      }

      if (lRet) {
        break;
      }

      if (mCanFail && !lRet) {
        WDDLOG_GRL(10000, "RegionAllocatorResource: Allocation failed. region={} alloc={} region_size={} free={}",
          mSegmentName, pSize, mRegion->GetSize(), mFree);
          WDDLOG_GRL(10000, "Memory region '{}' is too small, or there is a large backpressure.", mSegmentName);
        return nullptr;
      }

      while (true) {
        using namespace std::chrono_literals;
        WDDLOG_RL(1000, "RegionAllocatorResource: waiting to allocate a message. region={} alloc={} region_size={} free={}",
          mSegmentName, pSize, mRegion->GetSize(), mFree);
        WDDLOG_RL(1000, "Memory region '{}' is too small, or there is a large backpressure.", mSegmentName);
        if (lGen != mGeneration.load()) {
          break; // retry alloc
        } else {
          std::this_thread::sleep_for(10ms);
        }
      }
    }

    // check the running again
    if (!mRunning && !lRet) {
      WDDLOG("Memory segment '{}' is stopped. No allocations are possible.", mSegmentName);
      return nullptr;
    }

    mFree -= pSizeUp;
    assert (mFree >= 0);

    DDDLOG_GRL(5000, "DataRegionResource {} memory free={} allocated={}", mSegmentName, mFree, (mSegmentSize - mFree));

    // If the allocated message was aligned up, set a first byte after the buffer to 0
    if (pSizeUp > pSize) {
      reinterpret_cast<char*>(lRet)[pSize] = char(0xAA);
    }

    if constexpr (FREE_STRATEGY == eRefCount) {
      std::scoped_lock lLock(mAllocBlocksLock);
      const auto lStartI = reinterpret_cast<std::size_t>(lRet);
      mAllocBlocksMap.insert({ (lStartI + pSizeUp - 1), AllocBlock{ lStartI, pSizeUp, 0 } });
    }

    return lRet;
  }

private:
  inline
  void* try_alloc(const std::size_t pSize) {
    if (mLength >= pSize) {
      const auto lObjectPtr = mStart;

      mStart += pSize;
      mLength -= pSize;

      if (mLength == 0) {
        mStart = nullptr;
      }

      return lObjectPtr;
    }

    return nullptr;
  }

  bool try_reclaim(const std::size_t pSize) {
    // First declare any leftover memory as free
    std::scoped_lock lock(mReclaimLock);

    if (mLength > 0) {
      assert(mStart != nullptr);
      // NOTE: caller must hold mReclaimLock lock
      reclaimSHMMessage(mStart, mLength);
    }

    // invalidate the working extent
    mStart = nullptr;
    mLength = 0;

    if (mFreeRanges.empty()) {
      if (mFree != 0) {
        EDDLOG_GRL(1000, "DataRegionResource {} try_reclaim({}): FREE MAP is empty! free={}", mSegmentName, pSize, mFree);
      }
      return false;
    }

    auto lMaxIter = mFreeRanges.end();

    if constexpr (ALLOC_STRATEGY == eFindFirst) {
      for (auto lInt = mFreeRanges.begin(); lInt != mFreeRanges.end(); ++lInt) {
        if (lInt->first.upper() - lInt->first.lower() >= pSize) {
          lMaxIter = lInt;
          break;
        }
      }
    } else { /* eFindLongest */
      lMaxIter = std::max_element(mFreeRanges.begin(), mFreeRanges.end(),
        [](const auto& l, const auto& r) {
          return (l.first.upper() - l.first.lower()) < (r.first.upper() - r.first.lower());
        }
      );
    }

    if (lMaxIter == mFreeRanges.end()) {
      return false;
    }

    // check if the size is adequate
    const auto lFoundSize = lMaxIter->first.upper() - lMaxIter->first.lower();
    if (pSize > lFoundSize) {
      return false;
    }

    if (lMaxIter->second > 1) {
      EDDLOG_RL(1000, "RegionAllocator BUG: Overlapping interval found: ptr={:p} length={} overlaps={}",
        reinterpret_cast<char*>(lMaxIter->first.lower()), lFoundSize, lMaxIter->second);

      // erase this segment
      mFree -= lFoundSize;
      assert (mFree > 0);
      mFreeRanges.erase(lMaxIter);
      return false;
    }

    // return the extent
    mStart = reinterpret_cast<char*>(lMaxIter->first.lower());
    mLength = lFoundSize;
    mFreeRanges.erase(lMaxIter);

    {
      // estimated fragmentation
      static thread_local double sFree = 0.0;
      static thread_local double sNumFragments = 0.0;
      static thread_local double sFragmentation = 0.0;

      const std::size_t lFree = mFree;
      sFree = sFree * 0.75 + double(lFree) * 0.25;
      sFragmentation = sFragmentation * 0.75 + double(lFree - mLength)/double(lFree) * 0.25;
      sNumFragments = sNumFragments * 0.75 + double(mFreeRanges.iterative_size() + 1) * 0.25;

      DDDLOG_GRL(5000, "DataRegionResource {} estimated: free={:.4} num_fragments={:.4} fragmentation={:.4}",
        mSegmentName, sFree, sNumFragments, sFragmentation);
    }

    return true;
  }

  // pSize must be aligned up when deallocating
  // mReclaimLock must be held!
  void reclaimSHMMessage(const void* pData, const std::size_t pSize)
  {
    assert ((pSize / ALIGN * ALIGN) == pSize);
    assert (pSize >= ALIGN);

    mFreeRanges += std::make_pair(
      icl::discrete_interval<std::size_t>::right_open(
        reinterpret_cast<std::size_t>(pData), reinterpret_cast<std::size_t>(pData) + pSize),
        std::size_t(1)
    );

#if !defined(NDEBUG)
    for (const auto &lInt : mFreeRanges) {
      if (lInt.second > 1) {
        EDDLOG_RL(1000, "RegionAllocator BUG: Overlapping interval found on reclaim: region={} ptr={:p} length={} overlaps={}",
          mSegmentName, reinterpret_cast<char*>(lInt.first.lower()), lInt.first.upper() - lInt.first.lower(), lInt.second);
      }
    }
#endif
  }

  /// fields
  std::string mSegmentName;
  char *mSegmentAddr;        // Actual segment VM address
  char *mUCXSegmentAddr;     // UCX VM address mapping of the same segment
  std::size_t mSegmentSize;
  std::atomic_bool mRunning = false;
  bool mCanFail = false;

  fair::mq::TransportFactory &mTransport;
  std::unique_ptr<fair::mq::UnmanagedRegion> mRegion;

  char *mStart = nullptr;
  std::size_t mLength = 0;

  // free space accounting
  std::atomic_int64_t mFree = 0;
  std::atomic_uint64_t mGeneration = 0; // bump when free is finished, so that we don't retry allocs

  // two step reclaim to avoid lock contention in the allocation path
  std::mutex mReclaimLock;
    icl::interval_map<std::size_t, std::size_t> mFreeRanges;


  // track allocated blocks refcnt
  std::mutex mAllocBlocksLock;
    struct AllocBlock {
      std::size_t mStart;
      std::size_t mLength;
      std::uint64_t mRefCnt = 0;

      template<typename StartT, typename LenT>
      constexpr bool in_range(const StartT start, const LenT len) const {
        static_assert (sizeof(start) >= sizeof(std::size_t));
        static_assert (sizeof(len) >= sizeof(std::size_t));
        const std::size_t s = reinterpret_cast<std::size_t>(start);
        const std::size_t l = reinterpret_cast<std::size_t>(len);

        return ((s >= mStart) && ((s+l) <= (mStart+mLength)) );
      }
    };
    std::map<std::size_t, AllocBlock> mAllocBlocksMap; // key is (mStart + AlignSize)

};


using DataRegionAllocatorResource = RegionAllocatorResource<64, RegionAllocStrategy::eFindLongest, RegionATrackingStrategy::eRefCount>;
using HeaderRegionAllocatorResource = RegionAllocatorResource<alignof(o2::header::DataHeader), RegionAllocStrategy::eFindFirst>;


class MemoryResources {

public:
  MemoryResources() = delete;
  explicit MemoryResources(std::shared_ptr<fair::mq::TransportFactory> pShmTransport)
  : mShmTransport(pShmTransport) { }

  virtual ~MemoryResources() {
    // make sure to delete regions before dropping the transport
    mHeaderMemRes.reset();
    mDataMemRes.reset();
    mShmTransport.reset();
  }


  inline
  bool running() const { return mRunning; }

  void start() {
    if (mHeaderMemRes || mDataMemRes) {
      mRunning = true;
    }
  }

  void stop() {
    assert(mShmTransport);

    mRunning = false;

    if (mHeaderMemRes) {
      mHeaderMemRes->stop();
    }
    if (mDataMemRes) {
      mDataMemRes->stop();
    }
  }

  inline std::size_t freeHeader() const { return (running() && mHeaderMemRes) ? mHeaderMemRes->free() : std::size_t(0); }
  inline std::size_t freeData() const { return (running() && mDataMemRes) ? mDataMemRes->free() : std::size_t(0);  }

  inline std::size_t sizeHeader() const { return (running() && mHeaderMemRes) ? mHeaderMemRes->size() : std::size_t(0); }
  inline std::size_t sizeData() const { return (running() && mDataMemRes) ? mDataMemRes->size() : std::size_t(0);  }

  std::unique_ptr<HeaderRegionAllocatorResource> mHeaderMemRes;
  std::unique_ptr<DataRegionAllocatorResource> mDataMemRes;

  // shm transport
  std::shared_ptr<fair::mq::TransportFactory> mShmTransport;

private:
  bool mRunning = false;
};


class SyncMemoryResources : public MemoryResources {
public:
  SyncMemoryResources() = delete;
  explicit SyncMemoryResources(std::shared_ptr<fair::mq::TransportFactory> pShmTransport)
  : MemoryResources(pShmTransport) { }

  virtual ~SyncMemoryResources() {}

  template<typename T>
  inline
  fair::mq::MessagePtr newHeaderMessage(const T pData, const std::size_t pSize) {
    static_assert(std::is_pointer_v<T>, "Require pointer");
    assert(mHeaderMemRes);
    std::scoped_lock lock(mHdrLock);
    return mHeaderMemRes->NewFairMQMessage(pData, pSize);
  }

  inline
  fair::mq::MessagePtr newDataMessage(const std::size_t pSize) {
    assert(mDataMemRes);
    std::scoped_lock lock(mDataLock);
    return mDataMemRes->NewFairMQMessage(pSize);
  }

  template <typename T>
  inline
  fair::mq::MessagePtr newDataMessage(const T pData, const std::size_t pSize) {
    static_assert(std::is_pointer_v<T>, "Require pointer");
    assert(mDataMemRes);
    fair::mq::MessagePtr lMsg;
    {
      std::scoped_lock lock(mDataLock);
      lMsg = mDataMemRes->NewFairMQMessage(pSize);
    }

    if (lMsg) {
      memcpy(lMsg->GetData(), pData, pSize);
    }

    return lMsg;
  }

  inline
  bool replaceDataMessages(std::vector<fair::mq::MessagePtr> &pMsgs) {

    // create a new instance to support passing the same vect as in and out
    std::vector<fair::mq::MessagePtr> lNewMsgs(pMsgs.size());
    lNewMsgs.clear();
    bool lRetOk = true;

    { // allocate under one lock
      std::scoped_lock lock(mDataLock);
      for (const auto &lOrigMsg : pMsgs) {
        auto lMsg = mDataMemRes->NewFairMQMessage(lOrigMsg->GetSize());
        if (lMsg == nullptr) {
          lRetOk = false;
          break;
        }
        lNewMsgs.emplace_back(std::move(lMsg));
      }
    }

    // copy without holding allocator lock
    for (std::size_t i = 0; i < lNewMsgs.size(); i++) {
      memcpy(lNewMsgs[i]->GetData(), pMsgs[i]->GetData(), pMsgs[i]->GetSize());
      pMsgs[i] = std::move(lNewMsgs[i]);
    }

    return lRetOk;
  }

  template <typename OutIter>
  inline void allocDataBuffers(const std::vector<uint64_t> &pTxgSizes, OutIter pInsertIt) {
    std::scoped_lock lDataLock(mDataLock);

    for (const auto lSize : pTxgSizes) {
      *pInsertIt++ = std::move(mDataMemRes->do_allocate(lSize));
    }
  }

  template <typename OutIter>
  inline void allocHdrBuffers(const std::vector<uint64_t> &pHdrSizes, OutIter pInsertIt) {
    std::scoped_lock lDataLock(mHdrLock);

    for (const auto lSize : pHdrSizes) {
      *pInsertIt++ = std::move(mHeaderMemRes->NewFairMQMessage(lSize));
    }
  }

  template <typename OutIter>
  inline void fmqFromDataBuffers(const std::vector<std::pair<void*, std::size_t>> &pAllocs, OutIter pInsertIt) {
    // no locks needed for top level resource
    mDataMemRes->NewFairMQMessageFromPtr(pAllocs, pInsertIt);
  }


private:
  std::mutex mHdrLock;
  std::mutex mDataLock;
};

} /* o2::DataDistribution */

#endif /* DATADIST_MEMORY_UTILS_H_ */
