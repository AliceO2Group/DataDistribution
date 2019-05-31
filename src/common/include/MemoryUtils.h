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

  FMQUnsynchronizedPoolMemoryResource(FairMQDevice &pDev, FairMQChannel &pChan, const size_t pSize, const size_t pObjSize)
  : mChan(pChan),
    mSize(pSize),
    mObjectSize((pObjSize + sizeof(max_align_t) - 1) / sizeof(max_align_t) * sizeof(max_align_t))
  {

    mRegion = pDev.NewUnmanagedRegionFor(
      pChan.GetPrefix(), 0,
      pSize,
      [this](void* pRelData, size_t pRelSize, void* /* hint */) {
      // callback to be called when message buffers no longer needed by transport
      reclaimSHMMessage(pRelData, pRelSize);
    });

    // prepare header pointers
    unsigned char* lObj = static_cast<unsigned char*>(mRegion->GetData());
    memset(lObj, 0xAA, mRegion->GetSize());

    const std::size_t lObjectCnt = mRegion->GetSize() / mObjectSize;

    for (std::size_t i = 0; i < lObjectCnt; i++) {
      mAvailableObjects.push_back(lObj + i*mObjectSize);
    }
  }

  std::unique_ptr<FairMQMessage> NewFairMQMessage() {

    const auto lMem = allocate(0);

    if (lMem != nullptr) {

      return mChan.NewMessage(mRegion, lMem, mObjectSize);

    } else {
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

protected:
  virtual void* do_allocate(std::size_t , std::size_t) override
  {
    auto lRet = try_alloc();
    if (lRet)
      return lRet;

    // try to reclaim if possible
    if (try_reclaim()) {
      // try again
      return try_alloc();
    }

    return nullptr;
  }

  virtual void do_deallocate(void *, std::size_t, std::size_t) override
  {
    // Objects are only freed through SHM message reclaim callback.
    // This is intentionally noop.
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
  std::size_t mSize;
  std::size_t mObjectSize;

  std::vector<void*> mAvailableObjects;
  std::mutex mReclaimLock;

  // two step reclaim to avoid lock contention in the allocation path
  std::vector<void*> mReclaimedObjects;
};

}
} /* o2::DataDistribution */

#endif /* DATADIST_MEMORY_UTILS_H_ */
