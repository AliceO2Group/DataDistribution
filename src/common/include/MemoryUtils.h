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

#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQChannel.h>

#include <vector>
#include <mutex>
#include <memory>
#include <thread>
#include <chrono>

class DataHeader;
class FairMQUnmanagedRegion;

namespace o2
{
namespace DataDistribution
{

class FMQUnsynchronizedPoolMemoryResource : public boost::container::pmr::memory_resource
{

public:
  FMQUnsynchronizedPoolMemoryResource() = delete;

  FMQUnsynchronizedPoolMemoryResource(FairMQChannel &pChan,
                                      const std::size_t pSize, const std::size_t pObjSize)
  : mChan(pChan),
    mObjectSize(pObjSize),
    mAlignedSize((pObjSize + sizeof(max_align_t) - 1) / sizeof(max_align_t) * sizeof(max_align_t))
  {

    mRegion = pChan.NewUnmanagedRegion(pSize,
      [this](void* pRelData, size_t pRelSize, void* /* hint */) {
      // callback to be called when message buffers no longer needed by transport
      reclaimSHMMessage(pRelData, pRelSize);
    });

    // prepare header pointers
    unsigned char* lObj = static_cast<unsigned char*>(mRegion->GetData());
    memset(lObj, 0xAA, mRegion->GetSize());

    const std::size_t lObjectCnt = mRegion->GetSize() / mAlignedSize;

    for (std::size_t i = 0; i < lObjectCnt; i++) {
      mAvailableObjects.push_back(lObj + i * mAlignedSize);
    }
  }

  std::unique_ptr<FairMQMessage> NewFairMQMessage() {

    const auto lMem = allocate(0); // boost -> do_allocate(0) .. always return fixed object size (mObjectSize)

    if (lMem != nullptr) {
      return mChan.NewMessage(mRegion, lMem, mObjectSize);
    } else {
      // Log warning to increase the pool size
      static thread_local unsigned throttle = 0;
      if (++throttle > (1U << 18)) {
        LOG(WARNING) << "Header pool exhausted. Allocating from the global SHM pool.";
        throttle = 0;
      }

      return mChan.NewMessage(mObjectSize);
    }
  }

  std::unique_ptr<FairMQMessage> NewFairMQMessageFromPtr(void *pPtr) {
    assert(pPtr >= static_cast<byte*>(mRegion->GetData()) && pPtr < static_cast<byte*>(mRegion->GetData()) + mRegion->GetSize());

    return mChan.NewMessage(mRegion, pPtr, mObjectSize);
  }

  std::size_t objectSize() const { return mObjectSize; }

  inline auto allocator() { return boost::container::pmr::polymorphic_allocator<o2::byte>(this); }

protected:
  virtual void* do_allocate(std::size_t , std::size_t) override
  {
    unsigned long lAllocAttempt= 0;
    auto lRet = try_alloc();
    // we cannot fail! report problem if failing to allocate block often
    while (!lRet) {
      using namespace std::chrono_literals;

      if (++lAllocAttempt % 1000 == 0) {
        LOG(ERROR) << "FMQUnsynchronizedPoolMemoryResource: failing to get free block of "
                     << mObjectSize << " B, total region size: " << mRegion->GetSize() << " B";
        LOG(ERROR) << "Downstream components are creating back-pressure!";
      }

      std::this_thread::sleep_for(1ms);
      // try to reclaim if possible
      try_reclaim();
      // try again
      lRet = try_alloc();
    }

    return lRet;
  }

  virtual void do_deallocate(void *, std::size_t, std::size_t) override
  {
    // Objects are only freed through SHM message reclaim callback.
    // This is intentionally noop
    // NOTE: handled in reclaimSHMMessage()
  }

  virtual bool do_is_equal(const memory_resource &) const noexcept override { return false; }

private:

  void* try_alloc() {

    if (!mAvailableObjects.empty()) {
      auto lObjectPtr = mAvailableObjects.back();
      mAvailableObjects.pop_back();

      return lObjectPtr;
    }

    return nullptr;
  }

  bool try_reclaim() {

    std::scoped_lock lock(mReclaimLock);

    assert(mAvailableObjects.empty());

    mAvailableObjects = std::move(mReclaimedObjects);
    mReclaimedObjects.clear();

    return !mAvailableObjects.empty();
  }

  void reclaimSHMMessage(void* pData, size_t pSize)
  {
    (void) pSize;
    assert (pSize == mObjectSize);

    std::scoped_lock lock(mReclaimLock);
    mReclaimedObjects.push_back(pData);
  }

  FairMQChannel& mChan;

  std::unique_ptr<FairMQUnmanagedRegion> mRegion;
  std::size_t mObjectSize;
  std::size_t mAlignedSize;

  std::vector<void*> mAvailableObjects;
  std::mutex mReclaimLock;

  // two step reclaim to avoid lock contention in the allocation path
  std::vector<void*> mReclaimedObjects;
};

}
} /* o2::DataDistribution */

#endif /* DATADIST_MEMORY_UTILS_H_ */
