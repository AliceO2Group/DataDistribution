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

#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQChannel.h>

#include "DataDistLogger.h"

#include <vector>
#include <mutex>
#include <memory>
#include <thread>
#include <chrono>

#include <sys/mman.h>
#include <cstdlib>
#include <unistd.h>

class DataHeader;
class FairMQUnmanagedRegion;

namespace o2
{
namespace DataDistribution
{

static constexpr const char *ENV_SHM_PATH = "DATADIST_SHM_PATH";


template<size_t ALIGN = 64>
class RegionAllocatorResource
{

public:
  RegionAllocatorResource() = delete;

  RegionAllocatorResource(std::string pSegmentName, FairMQChannel &pChan,
                          std::size_t pSize, std::uint64_t pRegionFlags = 0)
  : mSegmentName(pSegmentName),
    mChan(pChan)
  {
    static_assert(ALIGN && !(ALIGN & (ALIGN - 1)), "Alignment must be power of 2");

    pSize = align_size_up(pSize);

    int lMapFlags = 0;
    std::string lSegmentRoot = "";

    // don't reserve swap space and try to lock the region
#if defined(MAP_NORESERVE) && defined(MAP_LOCKED)
    lMapFlags = MAP_NORESERVE | MAP_LOCKED;
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
          DDLOGF(fair::Severity::ERROR, "Hugetlbfs mountpoint does not exist. Not using huge pages. {}={}",
            ENV_SHM_PATH, lHugetlbfsPath);
          break;
        }

        // check if the hugetlbs is writeable
        if (0 != access(lHugetlbfsPath, W_OK)) {
          DDLOGF(fair::Severity::ERROR, "Hugetlbfs mountpoint is not writeable. "
            "Make sure the permissions are properly set. {}={}", ENV_SHM_PATH, lHugetlbfsPath);
          break;
        }

        lSegmentRoot = lHugetlbfsPath;
        lSegmentRoot += bfs::path::preferred_separator;
      } while (false);
    }

    DDLOGF(fair::Severity::INFO, "Creating new UnmanagedRegion name={} path={} size={} channel={}",
      mSegmentName, lSegmentRoot, pSize, pChan.GetName());

    mRegion = pChan.Transport()->CreateUnmanagedRegion(
      pSize,
      pRegionFlags,
      [this](const std::vector<FairMQRegionBlock>& pBlkVect) {

        static thread_local std::vector<FairMQRegionBlock> lSortedBlks;
        static thread_local double sMergeRatio = 0.5;

        lSortedBlks = pBlkVect;

        // sort and try to merge regions later
        std::sort(lSortedBlks.begin(), lSortedBlks.end(), [](FairMQRegionBlock &a, FairMQRegionBlock &b) {
          return a.ptr < b.ptr;
        });

        // merge the blocks
        std::uint64_t lMergedBlocks = 0;
        for (std::size_t i = 0; i < lSortedBlks.size() - 1; i++ ) {
          for (std::size_t j = i+1; j < lSortedBlks.size(); j++ ) {
            if (reinterpret_cast<char*>(lSortedBlks[i].ptr) + align_size_up(lSortedBlks[i].size) == lSortedBlks[j].ptr) {
              lSortedBlks[i].size = align_size_up(lSortedBlks[i].size) + align_size_up(lSortedBlks[j].size);
              lSortedBlks[j].ptr = nullptr;
              lMergedBlocks++;
            } else {
              i = j;
              break;
            }
          }
        }

        // callback to be called when message buffers no longer needed by transports
        std::scoped_lock lock(mReclaimLock);

        std::int64_t lReclaimed = 0;
        for (const auto &lBlk : lSortedBlks) {

          if (!lBlk.ptr) { // merged
              continue;
          }

          // we can merge with alignied sizes
          lReclaimed += align_size_up(lBlk.size);
          reclaimSHMMessage(lBlk.ptr, lBlk.size);
        }

        mFree.fetch_add(lReclaimed, std::memory_order_relaxed);

        // weighted average merge ratio
        sMergeRatio = sMergeRatio * 0.75 + double(lMergedBlocks) / double(pBlkVect.size()) * 0.25;
        DDLOGF_RL(5000, fair::Severity::DEBUG, "Memory segment '{}'::block merging ratio average={:.4}",
          mSegmentName, sMergeRatio);
      },
      lSegmentRoot.c_str(),
      lMapFlags
    );

    if (!mRegion) {
      DDLOGF(fair::Severity::FATAL, "Creation of new memory region failed. name={} size={} path={} channel={}",
      mSegmentName, pSize, lSegmentRoot, pChan.GetName());
      throw std::bad_alloc();
    }

    mStart = static_cast<char*>(mRegion->GetData());
    mSegmentSize = mRegion->GetSize();
    mLength = mRegion->GetSize();

    memset(mStart, 0xAA, mLength);

    mFree = mSegmentSize;

    // start the allocations
    mRunning = true;
  }

  inline
  std::unique_ptr<FairMQMessage> NewFairMQMessage(std::size_t pSize) {
    auto* lMem = do_allocate(pSize, ALIGN);
    if (lMem) {
      return mChan.NewMessage(mRegion, lMem, pSize);
    } else {
      return nullptr;
    }
  }

  inline
  std::unique_ptr<FairMQMessage> NewFairMQMessageFromPtr(void *pPtr, const std::size_t pSize) {
    assert(pPtr >= static_cast<char*>(mRegion->GetData()));
    assert(static_cast<char*>(pPtr)+pSize <= static_cast<char*>(mRegion->GetData()) + mRegion->GetSize());

    return mChan.NewMessage(mRegion, pPtr, pSize);
  }

  void stop() {
    std::scoped_lock lock(mReclaimLock);
    mRunning = false;
  }

protected:
  // NOTE: we align sizes of returned messages, but keep the exact size for allocation
  //       otherwise the shm messages would be larger than requested
  static constexpr inline
  std::size_t align_size_up(const std::size_t pSize) {
    return (pSize + ALIGN - 1) / ALIGN * ALIGN;
  }

  void* do_allocate(std::size_t pSize, std::size_t /* pAlign */)
  {
#if !defined(NDEBUG)
    static const std::thread::id d_sThisId = std::this_thread::get_id();
    if (d_sThisId != std::this_thread::get_id()) {
      DDLOGF_RL(1000, fair::Severity::ERROR, "Allocation from RegionAllocatorResource {} is not thread safe",
        mSegmentName);
    }
#endif

    if (!mRunning) {
      return nullptr;
    }

    if (pSize == 0) {
      // return last address of the segment
      return reinterpret_cast<char*>(mRegion->GetData()) + mRegion->GetSize();
    }

    // align up
    pSize = align_size_up(pSize);

    auto lRet = try_alloc(pSize);
    // we cannot fail! report problem if failing to allocate block often
    while (!lRet && mRunning) {
      // try to reclaim if possible
      if (try_reclaim(pSize)) {
        // try again
        lRet = try_alloc(pSize);
      }

      if (!lRet) {
        using namespace std::chrono_literals;

        DDLOGF_RL(1000, fair::Severity::WARNING,
          "RegionAllocatorResource: waiting to allocate a message. region={} alloc={} region_size={} free={} ",
          mSegmentName, pSize, mRegion->GetSize(), mFree.load(std::memory_order_acquire));
        DDLOGF_RL(1000, fair::Severity::WARNING, "Memory region '{}' is too small, or there is a large backpressure.",
          mSegmentName);

        std::this_thread::sleep_for(5ms);
      }
    }

    // check the running again
    if (!mRunning && !lRet) {
      DDLOGF(fair::Severity::WARNING, "Memory segment '{}' is stopped. No allocations are possible.", mSegmentName);
      return nullptr;
    }

    mFree.fetch_sub(pSize, std::memory_order_relaxed);

    // Todo: find a better place for this reporting
    static std::size_t sLogRateLimit = 0;
    if (sLogRateLimit++ % 1024 == 0) {
      const auto lFree = mFree.load(std::memory_order_acquire);
      DDLOGF_RL(2000, fair::Severity::DEBUG, "DataRegionResource {} memory free={} allocated={}",
        mSegmentName, lFree, (mSegmentSize - lFree));
    }

    return lRet;
  }

private:
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

    if (mFrees.empty()) {
      return false;
    }

    // find the largest free extent and return it if the size is adequate
    auto lMaxIter = std::max_element(std::begin(mFrees), std::end(mFrees),
      [](const auto& l, const auto& r) { return l.second < r.second; });

    // check if the size is adequate
    if (pSize > lMaxIter->second) {
      return false;
    }

    // return the extent
    mStart = const_cast<char*>(lMaxIter->first);
    mLength = lMaxIter->second;
    mFrees.erase(lMaxIter);

    return true;
  }

  void reclaimSHMMessage(void* pData, size_t pSize)
  {
    // align up
    pSize = align_size_up(pSize);

    const char *lData = reinterpret_cast<const char*>(pData);

    // push object to the free map. Try to merge nodes
    const auto lIter = mFrees.lower_bound(lData);
    bool lInserted = false;

#if !defined(NDEBUG)
    if (lIter != mFrees.end()) {
      if (lIter->first <= lData) {
        DDLOGF(fair::Severity::ERROR, "iter={:p}, data={:p}, Free map:", lIter->first, lData);

        for (const auto &lit : mFrees) {
          DDLOGF(fair::Severity::ERROR, " iter={:p}, size={}", lit.first, lit.second);
        }
      }
      assert(lIter->first > lData); // we cannot have this exact value in the free list
    }
#endif

    // check if we can merge with the previous
    if (!mFrees.empty() && lIter != mFrees.begin()) {
      auto lPrev = std::prev(lIter);

      if ((lPrev->first + lPrev->second) == lData) {
        lPrev->second += pSize;
        lInserted = true;

        // check if we also can merge with the next (lIter)
        if (lIter != mFrees.end()) {
          if ((lPrev->first + lPrev->second) == lIter->first) {
            lPrev->second += lIter->second;
            mFrees.erase(lIter);
          }
        }
      }
    }

    if (!lInserted) {
      // insert the new range
      auto lIt = mFrees.emplace_hint(lIter, lData, pSize);

      if (lIt->second != pSize) {
        DDLOGF(fair::Severity::ERROR, "BUG: RegionAllocatorResource: REPEATED INSERT!!! "
          " {:p} : {}, original size: {}", lIt->first, pSize, lIt->second);
      }

      // check if we can merge with the next
      auto lNextIt = std::next(lIt);
      if (lNextIt != mFrees.cend()) {
        if ((lData + pSize) == lNextIt->first) {
          lIt->second += lNextIt->second;
          mFrees.erase(lNextIt);
        }
      }
    }
  }

  /// fields
  std::string mSegmentName;
  std::size_t mSegmentSize;
  std::atomic_bool mRunning = true;

  FairMQChannel& mChan;
  std::unique_ptr<FairMQUnmanagedRegion> mRegion;

  char *mStart = nullptr;
  std::size_t mLength = 0;

  // two step reclaim to avoid lock contention in the allocation path
  std::mutex mReclaimLock;
  std::map<const char*, std::size_t> mFrees; // keep all returned blocks

  // free space accounting
  std::atomic_int64_t mFree = 0;
};


class MemoryResources {

public:
  inline
  FairMQMessagePtr newHeaderMessage(const std::size_t pSize) {
    return mHeaderMemRes->NewFairMQMessage(pSize);
  }

  inline
  FairMQMessagePtr getDataMessage(const std::size_t pSize) {
    return mDataMemRes->NewFairMQMessage(pSize);
  }

  inline
  bool running() { return mRunning == true; }

  void stop() {
    mRunning = false;

    if (mHeaderMemRes) {
      mHeaderMemRes->stop();
    }
    if (mDataMemRes) {
      mDataMemRes->stop();
    }
  }

  std::unique_ptr<RegionAllocatorResource<alignof(o2::header::DataHeader)>> mHeaderMemRes;
  std::unique_ptr<RegionAllocatorResource<64>> mDataMemRes;

private:
  bool mRunning = true;
};

}
} /* o2::DataDistribution */

#endif /* DATADIST_MEMORY_UTILS_H_ */
