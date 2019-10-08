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

  ~ConcurrentContainerImpl()
  {
    stop();
  }

  void stop()
  {
    std::unique_lock<std::mutex> lLock(mLock);
    mRunning = false;
    lLock.unlock();
    mCond.notify_all();
  }

  void flush()
  {
    std::unique_lock<std::mutex> lLock(mLock);
    mContainer.clear();
    lLock.unlock();
    mCond.notify_all();
  }

  template <typename... Args>
  void push(Args&&... args)
  {
    std::unique_lock<std::mutex> lLock(mLock);

    if (type == eFIFO) {
      mContainer.emplace_back(std::forward<Args>(args)...);
    } else if (type == eLIFO) {
      mContainer.emplace_front(std::forward<Args>(args)...);
    }

    lLock.unlock(); // reduce contention
    mCond.notify_one();
  }

  bool pop(T& d)
  {
    std::unique_lock<std::mutex> lLock(mLock);
    while (mContainer.empty() && mRunning) {
      mCond.wait(lLock);
    }

    if (!mRunning && mContainer.empty())
      return false;

    assert(!mContainer.empty());
    d = std::move(mContainer.front());
    mContainer.pop_front();
    return true;
  }

  template <class OutputIt>
  unsigned long pop_n(const unsigned long pCnt, OutputIt pDstIter)
  {
    std::unique_lock<std::mutex> lLock(mLock);
    while (mContainer.empty() && mRunning) {
      mCond.wait(lLock);
    }

    if (!mRunning && mContainer.empty())
      return false; // should stop

    assert(!mContainer.empty());

    unsigned long ret = std::min(mContainer.size(), pCnt);
    std::move(std::begin(mContainer), std::begin(mContainer) + ret, pDstIter);
    mContainer.erase(std::begin(mContainer), std::begin(mContainer) + ret);
    return ret;
  }

  bool try_pop(T& d)
  {
    std::unique_lock<std::mutex> lLock(mLock);
    if (mContainer.empty()) {
      return false;
    }

    d = std::move(mContainer.front());
    mContainer.pop_front();
    return true;
  }

  template <class OutputIt>
  unsigned long try_pop_n(const unsigned long pCnt, OutputIt pDstIter)
  {
    std::unique_lock<std::mutex> lLock(mLock);
    if (mContainer.empty()) {
      return 0;
    }

    unsigned long ret = std::min(mContainer.size(), pCnt);
    std::move(std::begin(mContainer), std::begin(mContainer) + ret, pDstIter);
    mContainer.erase(std::begin(mContainer), std::begin(mContainer) + ret);
    return ret;
  }

  std::size_t size() const
  {
    std::unique_lock<std::mutex> lLock(mLock);
    return mContainer.size();
  }

  bool is_running() const { return mRunning; }

 private:
  std::deque<T> mContainer;
  mutable std::mutex mLock;
  std::condition_variable mCond;
  bool mRunning = true;
};

} /* namespace impl*/

///
///  ConcurrentContainerImpl specializations for o2::DataDistribution
///

// concurrent Queue (FIFO)
template <class T>
using ConcurrentFifo = impl::ConcurrentContainerImpl<T, impl::eFIFO>;

// concurrent Stack (LIFO)
template <class T>
using ConcurrentLifo = impl::ConcurrentContainerImpl<T, impl::eLIFO>;

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

  template <typename... Args>
  bool queue(unsigned pStage, Args&&... args)
  {
    assert(pStage < mPipelineQueues.size());
    auto lNextStage = getNextPipelineStage(pStage);
    assert((lNextStage <= mPipelineQueues.size()) && "next stage larger than expected");

    // NOTE: (lNextStage == mPipelineQueues.size()) is the drop queue
    if (lNextStage < mPipelineQueues.size()) {
      const auto lSize = ++mPipelinedSize;
      mPipelineQueues[lNextStage].push(std::forward<Args>(args)...);
      mPipelinedSizeSamples.Fill(lSize);
      return true;
    }
    return false;
  }

  T dequeue(unsigned pStage)
  {
    T t;
    mPipelineQueues[pStage].pop(t);
    mPipelinedSize--;
    return std::move(t);
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
