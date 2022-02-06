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

#include <boost/container/pmr/memory_resource.hpp>
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

template<size_t ALIGN = 64>
class RegionAllocatorResource
{

public:
  RegionAllocatorResource() = delete;

  RegionAllocatorResource(const std::string &pSegmentName, const std::optional<uint16_t> pSegmentId, std::size_t pSize,
                          FairMQTransportFactory& pShmTrans,
                          const RegionAllocStrategy pStrategy, std::uint64_t pRegionFlags = 0,
                          bool pCanFail = false)
  : mSegmentName(pSegmentName),
    mStrategy(pStrategy),
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

    auto lReclaimFn = [this, lZeroShmMemory, lZeroCheckShmMemory](const std::vector<FairMQRegionBlock>& pBlkVect) {
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
        if (lZeroCheckShmMemory && lASize > lInt.size) {
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
            EDDLOG("CreateUnmanagedRegion reclaim BUG! Multiple overlapping intervals:");
            for (const auto &i : lIntMap) {
              EDDLOG("- [{},{}) : count={}", i.first.lower(), i.first.upper(), i.second);
            }

            continue; // skip the overlapping thing
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

  inline
  std::unique_ptr<FairMQMessage> NewFairMQMessage(std::size_t pSize) {
    auto* lMem = do_allocate(pSize);
    if (lMem) {
      return mTransport.CreateMessage(mRegion, lMem, pSize);
    } else {
      return nullptr;
    }
  }

  template<typename T>
  inline
  std::unique_ptr<FairMQMessage> NewFairMQMessage(const T pData, const std::size_t pSize) {
    static_assert(std::is_pointer_v<T>, "Require pointer");
    auto* lMem = do_allocate(pSize);
    if (lMem) {
      std::memcpy(lMem, pData, pSize);
      return mTransport.CreateMessage(mRegion, lMem, pSize);
    } else {
      return nullptr;
    }
  }

  inline
  std::unique_ptr<FairMQMessage> NewFairMQMessageFromPtr(void *pPtr, const std::size_t pSize) {
    assert(pPtr >= static_cast<char*>(mRegion->GetData()));
    assert(static_cast<char*>(pPtr)+pSize <= static_cast<char*>(mRegion->GetData()) + mRegion->GetSize());

    return mTransport.CreateMessage(mRegion, pPtr, pSize);
  }

  void stop() {
    std::scoped_lock lock(mReclaimLock);
    mRunning = false;
  }

  std::size_t free() const { return mFree; }
  std::size_t size() const { return mSegmentSize; }

  bool running() const { return mRunning; }

protected:
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
      return reinterpret_cast<char*>(mRegion->GetData()) + mRegion->GetSize();
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
        WDDLOG_RL(1000, "RegionAllocatorResource: Allocation failed. region={} alloc={} region_size={} free={}",
          mSegmentName, pSize, mRegion->GetSize(), mFree);
          WDDLOG_RL(1000, "Memory region '{}' is too small, or there is a large backpressure.", mSegmentName);
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
          std::this_thread::sleep_for(20ms);
        }
      }
    }

    // check the running again
    if (!mRunning && !lRet) {
      WDDLOG("Memory segment '{}' is stopped. No allocations are possible.", mSegmentName);
      return nullptr;
    }

    mFree -= pSizeUp;
    assert (mFree > 0);

    static thread_local std::size_t sLogRateLimit = 0;
    if (sLogRateLimit++ % 1024 == 0) {
      const std::int64_t lFree = mFree;
      DDDLOG_GRL(2000, "DataRegionResource {} memory free={} allocated={}", mSegmentName, lFree, (mSegmentSize - lFree));
    }

    // If the allocated message was aligned up, set a first byte after the buffer to 0
    if (pSizeUp > pSize) {
      reinterpret_cast<char*>(lRet)[pSize] = char(0xAA);
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
      WDDLOG_GRL(1000, "DataRegionResource {} try_reclaim({}): FREE MAP is empty! free={}", mSegmentName, pSize, mFree);
      return false;
    }

    auto lMaxIter = mFreeRanges.end();

    if (mStrategy == eFindFirst) {
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
    mFreeRanges += std::make_pair(
      icl::discrete_interval<std::size_t>::right_open(
        reinterpret_cast<std::size_t>(pData), reinterpret_cast<std::size_t>(pData) + pSize),
        std::size_t(1)
    );

#if !defined(NDEBUG)
    for (const auto &lInt : mFreeRanges) {
      if (lInt.second > 1) {
        EDDLOG_RL(1000, "RegionAllocator BUG: Overlapping interval found on reclaim: ptr={:p} length={} overlaps={}",
          reinterpret_cast<char*>(lInt.first.lower()), lInt.first.upper() - lInt.first.lower(), lInt.second);
      }
    }
#endif
  }

  /// fields
  std::string mSegmentName;
  std::size_t mSegmentSize;
  std::atomic_bool mRunning = false;
  RegionAllocStrategy mStrategy;
  bool mCanFail = false;

  FairMQTransportFactory &mTransport;
  std::unique_ptr<FairMQUnmanagedRegion> mRegion;

  char *mStart = nullptr;
  std::size_t mLength = 0;

  // free space accounting
  std::atomic_int64_t mFree = 0;
  std::atomic_uint64_t mGeneration = 0; // bump when free is finished, so that we don't retry allocs

  // two step reclaim to avoid lock contention in the allocation path
  std::mutex mReclaimLock;
  icl::interval_map<std::size_t, std::size_t> mFreeRanges;
};


class MemoryResources {

public:
  MemoryResources() = delete;
  explicit MemoryResources(std::shared_ptr<FairMQTransportFactory> pShmTransport)
  : mShmTransport(pShmTransport) { }

  virtual ~MemoryResources() {
    // make sure to delete regions before dropping the transport
    mHeaderMemRes.reset();
    mDataMemRes.reset();
    mShmTransport.reset();
  }

  template<typename T>
  inline
  FairMQMessagePtr newHeaderMessage(const T pData, const std::size_t pSize) {
    static_assert(std::is_pointer_v<T>, "Require pointer");
    assert(mHeaderMemRes);
    return mHeaderMemRes->NewFairMQMessage(reinterpret_cast<const char*>(pData), pSize);
  }

  inline
  FairMQMessagePtr newDataMessage(const std::size_t pSize) {
    assert(mDataMemRes);
    return mDataMemRes->NewFairMQMessage(pSize);
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

  std::unique_ptr<RegionAllocatorResource<alignof(o2::header::DataHeader)>> mHeaderMemRes;
  std::unique_ptr<RegionAllocatorResource<64>> mDataMemRes;

  // shm transport
  std::shared_ptr<FairMQTransportFactory> mShmTransport;

private:
  bool mRunning = false;
};


class SyncMemoryResources : public MemoryResources {
public:
  SyncMemoryResources() = delete;
  explicit SyncMemoryResources(std::shared_ptr<FairMQTransportFactory> pShmTransport)
  : MemoryResources(pShmTransport) { }

  virtual ~SyncMemoryResources() {}

  template<typename T>
  inline
  FairMQMessagePtr newHeaderMessage(const T pData, const std::size_t pSize) {
    static_assert(std::is_pointer_v<T>, "Require pointer");
    assert(mHeaderMemRes);
    std::scoped_lock lock(mHdrLock);
    return mHeaderMemRes->NewFairMQMessage(pData, pSize);
  }

  inline
  FairMQMessagePtr newDataMessage(const std::size_t pSize) {
    assert(mDataMemRes);
    std::scoped_lock lock(mDataLock);
    return mDataMemRes->NewFairMQMessage(pSize);
  }

  template <typename T>
  inline
  FairMQMessagePtr newDataMessage(const T pData, const std::size_t pSize) {
    static_assert(std::is_pointer_v<T>, "Require pointer");
    assert(mDataMemRes);
    FairMQMessagePtr lMsg;
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
  void newDataMessages(const std::vector<FairMQMessagePtr> &pSrcMsgs, std::vector<FairMQMessagePtr> &pDstMsgs) {

    // create a new instance to support passing the same vect as in and out
    std::vector<FairMQMessagePtr> lNewMsgs(pSrcMsgs.size());
    lNewMsgs.clear();

    { // allocate under one lock
      std::scoped_lock lock(mDataLock);
      for (const auto &lOrigMsg : pSrcMsgs) {
        lNewMsgs.emplace_back( mDataMemRes->NewFairMQMessage(lOrigMsg->GetSize()) );
      }
    }

    // copy without holding allocator lock
    for (std::size_t i = 0; i < pSrcMsgs.size(); i++) {
      memcpy(lNewMsgs[i]->GetData(), pSrcMsgs[i]->GetData(), pSrcMsgs[i]->GetSize());
    }

    pDstMsgs = std::move(lNewMsgs);
  }

private:
  std::mutex mHdrLock;
  std::mutex mDataLock;
};

} /* o2::DataDistribution */

#endif /* DATADIST_MEMORY_UTILS_H_ */
