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

#ifndef ALICEO2_CONCURRENT_QUEUE_H_
#define ALICEO2_CONCURRENT_QUEUE_H_

#include <cassert>
#include <atomic>
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <iterator>
#include <chrono>

#include <Utilities.h>

namespace o2
{
namespace DataDistribution
{

namespace impl
{

/// Concurrent (thread-safe) container adapter for FIFO/LIFO data structures
enum QueueType {
  eFIFO,
  eLIFO
};

template <typename T, QueueType type>
class ConcurrentContainerImpl
{
 public:
  typedef T value_type;

  ConcurrentContainerImpl()
  : mImpl(std::make_unique<QueueInternals>()) { }

  ConcurrentContainerImpl(ConcurrentContainerImpl &&) = default;

  ~ConcurrentContainerImpl() { stop(); }

  void stop()
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    mImpl->mRunning = false;
    lLock.unlock();
    mImpl->mCond.notify_all();
  }

  std::size_t flush()
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    auto lCount = mImpl->mContainer.size();
    mImpl->mContainer.clear();
    lLock.unlock();
    mImpl->mCond.notify_all();
    return lCount;
  }

  // push a new element to the queue, while in the running state
  // return false (fail) if not running
  template <typename... Args>
  bool push(Args&&... args)
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    if (!mImpl->mRunning) {
      mImpl->mCond.notify_all(); // just in case someone is waiting
      return false;
    }

    if constexpr (type == eFIFO) {
      mImpl->mContainer.emplace_back(std::forward<Args>(args)...);
    } else if constexpr (type == eLIFO) {
      mImpl->mContainer.emplace_front(std::forward<Args>(args)...);
    } else {
      static_assert("Unknown queuing strategy.");
    }

    lLock.unlock(); // reduce contention
    mImpl->mCond.notify_one();
    return true;
  }

  // push a new element to the queue, while in the running state
  // The oldest element will be dropped if over capacity
  // return false (fail) if not running. Capacity is not strictly enforced if the container size is already over cap.
  template <typename... Args>
  bool push_capacity(const std::size_t pCap, Args&&... args)
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    if (!mImpl->mRunning) {
      mImpl->mCond.notify_all(); // just in case someone is waiting
      return false;
    }

    if ((pCap > 0) && (pCap <= mImpl->mContainer.size())) {
      if constexpr (type == eFIFO) {
        mImpl->mContainer.pop_front();
      } else if constexpr (type == eLIFO) {
        mImpl->mContainer.pop_back();
      }
    }

    if constexpr (type == eFIFO) {
      mImpl->mContainer.emplace_back(std::forward<Args>(args)...);
    } else if constexpr (type == eLIFO) {
      mImpl->mContainer.emplace_front(std::forward<Args>(args)...);
    } else {
      static_assert("Unknown queuing strategy.");
    }

    lLock.unlock(); // reduce contention
    mImpl->mCond.notify_one();
    return true;
  }

  // pop an element from the queue. Caller will block while the queue is running
  // returns true on success
  bool pop(T& d)
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    while (mImpl->mContainer.empty() && mImpl->mRunning) {
      mImpl->mCond.wait(lLock);
    }

    if (!mImpl->mRunning && mImpl->mContainer.empty())
      return false;

    assert(!mImpl->mContainer.empty());
    d = std::move(mImpl->mContainer.front());
    mImpl->mContainer.pop_front();
    return true;
  }

  bool pop_wait_for(T& d, const std::chrono::microseconds &us)
  {
    const auto lWaitUntil = std::chrono::system_clock::now() + us;
    {
      std::unique_lock<std::mutex> lLock(mImpl->mLock);
      if (mImpl->mContainer.empty() && (mImpl->mRunning)) {
        // wait until timeout is reached, or the queue has new elements
        while (mImpl->mRunning && (mImpl->mCond.wait_until(lLock, lWaitUntil) != std::cv_status::timeout)) {
          if (!mImpl->mContainer.empty()) {
            break;
          }
        }
        if (!mImpl->mRunning || mImpl->mContainer.empty()) {
          return false;
        }
      }
    }

    return try_pop(d);
  }


  template <class OutputIt>
  std::size_t pop_n(const unsigned long pCnt, OutputIt pDstIter)
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    while (mImpl->mContainer.empty() && mImpl->mRunning) {
      mImpl->mCond.wait(lLock);
    }

    if (!mImpl->mRunning && mImpl->mContainer.empty())
      return 0; // should stop

    assert(!mImpl->mContainer.empty());

    std::size_t ret = std::min(mImpl->mContainer.size(), pCnt);
    std::copy_n(std::make_move_iterator(mImpl->mContainer.begin()), ret, pDstIter);
    mImpl->mContainer.erase(std::begin(mImpl->mContainer), std::begin(mImpl->mContainer) + ret);
    return ret;
  }

  bool try_pop(T& d)
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    if (mImpl->mContainer.empty()) {
      return false;
    }

    d = std::move(mImpl->mContainer.front());
    mImpl->mContainer.pop_front();
    return true;
  }

  template <class OutputIt>
  std::size_t try_pop_n(const std::size_t pCnt, OutputIt pDstIter)
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    if (mImpl->mContainer.empty()) {
      return 0;
    }

    const std::size_t ret = std::min(mImpl->mContainer.size(), pCnt);
    std::copy_n(std::make_move_iterator(mImpl->mContainer.begin()), ret, pDstIter);
    mImpl->mContainer.erase(std::begin(mImpl->mContainer), std::begin(mImpl->mContainer) + ret);
    return ret;
  }

  std::size_t size() const
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    return mImpl->mContainer.size();
  }

  bool empty() const { return size() == 0; }

  bool is_running() const { return mImpl->mRunning; }

  private:
  struct QueueInternals {
    std::deque<T> mContainer;
    mutable std::mutex mLock;
    std::condition_variable mCond;
    bool mRunning = true;
  };

  std::unique_ptr<QueueInternals> mImpl;
};

} /* namespace impl*/

///
///  ConcurrentContainerImpl specializations for o2::DataDistribution
///

// concurrent Queue (FIFO)
template <class T>
using ConcurrentFifo = impl::ConcurrentContainerImpl<T, impl::eFIFO>;

template <class T>
using ConcurrentQueue = ConcurrentFifo<T>;

// concurrent Stack (LIFO)
template <class T>
using ConcurrentLifo = impl::ConcurrentContainerImpl<T, impl::eLIFO>;

template <class T>
using ConcurrentStack = ConcurrentLifo<T>;

///
///  Pipeline handler with input and output ConcurrentContainer queue/stack
///
template <
  typename T,
  typename = std::enable_if_t<std::is_move_assignable<T>::value>>
class IFifoPipeline
{
 public:
  IFifoPipeline() = delete;

  IFifoPipeline(unsigned pNoStages)
    : mPipelineQueues(pNoStages),
      mPipelinedSizeSamples(0)
  {
  }

  virtual ~IFifoPipeline() {}

  void stopPipeline()
  {
    for (auto& lQueue : mPipelineQueues) {
      lQueue.stop();
    }
  }

  void clearPipeline()
  {
    for (auto& lQueue : mPipelineQueues) {
      lQueue.flush();
    }
  }

  template <typename... Args>
  bool queue(unsigned pStage, Args&&... args)
  {
    assert(pStage < mPipelineQueues.size());
    auto lNextStage = getNextPipelineStage(pStage);
    assert((lNextStage <= mPipelineQueues.size()) && "next stage larger than expected");

    // NOTE: (lNextStage == mPipelineQueues.size()) is the drop queue
    if (lNextStage < mPipelineQueues.size()) {
      if (!mPipelineQueues[lNextStage].is_running()) {
        return false;
      }

      const auto lSize = ++mPipelinedSize;
      mPipelineQueues[lNextStage].push(std::forward<Args>(args)...);
      mPipelinedSizeSamples.Fill(lSize);
      return true;
    }
    return false;
  }

  // notify the receiver the queue is closed
  void close(unsigned pStage)
  {
    assert(pStage < mPipelineQueues.size());
    auto lNextStage = getNextPipelineStage(pStage);
    assert((lNextStage <= mPipelineQueues.size()) && "next stage larger than expected");

    if (lNextStage < mPipelineQueues.size()) {
      mPipelineQueues[lNextStage].stop();
    }
  }

  // notify the receiver the queue is closed and flush the queue
  std::size_t flush(unsigned pStage)
  {
    assert(pStage < mPipelineQueues.size());
    auto lNextStage = getNextPipelineStage(pStage);
    assert((lNextStage <= mPipelineQueues.size()) && "next stage larger than expected");

    std::size_t lCount = 0;
    if (lNextStage < mPipelineQueues.size()) {
      lCount = mPipelineQueues[lNextStage].flush();
    }
    return lCount;
  }

  T dequeue(unsigned pStage)
  {
    T t;
    mPipelineQueues[pStage].pop(t);
    mPipelinedSize--;
    return t;
  }

  bool try_pop(unsigned pStage)
  {
    T t;
    return mPipelineQueues[pStage].try_pop(t);
  }

  long getPipelineSize() const noexcept { return mPipelinedSize; }

  const auto& getPipelinedSizeSamples() const noexcept { return mPipelinedSizeSamples; }

 protected:
  virtual unsigned getNextPipelineStage(unsigned pStage) = 0;

  std::atomic_long mPipelinedSize = 0;
  std::vector<o2::DataDistribution::ConcurrentFifo<T>> mPipelineQueues;

  RunningSamples<long> mPipelinedSizeSamples;
};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_CONCURRENT_QUEUE_H_ */
