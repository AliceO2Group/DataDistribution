// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

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

namespace o2::DataDistribution
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

  void start()
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    mImpl->mContainer.clear();
    mImpl->mRunning = true;
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
    }

    static_assert(type == eFIFO || type == eLIFO, "Unknown queuing strategy.");


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
    }
    static_assert(type == eFIFO || type == eLIFO, "Unknown queuing strategy.");

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

  std::optional<T> pop()
  {
    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    while (mImpl->mContainer.empty() && mImpl->mRunning) {
      mImpl->mCond.wait(lLock);
    }

    if (!mImpl->mRunning && mImpl->mContainer.empty()) {
      return std::nullopt;
    }

    assert(!mImpl->mContainer.empty());
    auto d = std::make_optional<T>(std::move(mImpl->mContainer.front()));
    mImpl->mContainer.pop_front();
    return d;
  }

  template <typename duration>
  bool pop_wait_for(T& d, const duration &dur)
  {
    const auto lWaitUntil = std::chrono::system_clock::now() + dur;
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

  template <typename duration>
  std::optional<T> pop_wait_for(const duration &dur)
  {
    const auto lWaitUntil = std::chrono::system_clock::now() + dur;

    std::unique_lock<std::mutex> lLock(mImpl->mLock);
    if (mImpl->mContainer.empty() && (mImpl->mRunning)) {
      // wait until timeout is reached, or the queue has new elements
      while (mImpl->mRunning && (mImpl->mCond.wait_until(lLock, lWaitUntil) != std::cv_status::timeout)) {
        if (!mImpl->mContainer.empty()) {
          break;
        }
      }
    }

    if (!mImpl->mContainer.empty()) {
      auto d = std::make_optional<T>(std::move(mImpl->mContainer.front()));
      mImpl->mContainer.pop_front();
      return d;
    }

    return std::nullopt;
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
    : mPipelineQueues(pNoStages)
  {
  }

  virtual ~IFifoPipeline() {}

  void stopPipeline()
  {
    for (auto& lQueue : mPipelineQueues) {
      lQueue.flush();
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
      return mPipelineQueues[lNextStage].push(std::forward<Args>(args)...);
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
    return t;
  }

  template <typename duration>
  std::optional<T> dequeue_for(const unsigned pStage, const duration &pWaitDur)
  {
    return mPipelineQueues[pStage].pop_wait_for(pWaitDur);
  }

  bool try_pop(unsigned pStage)
  {
    T t;
    return mPipelineQueues[pStage].try_pop(t);
  }

  bool is_running(const unsigned pStage) const { return mPipelineQueues[pStage].is_running(); }

 protected:
  virtual unsigned getNextPipelineStage(unsigned pStage) = 0;

  std::vector<o2::DataDistribution::ConcurrentFifo<T>> mPipelineQueues;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_CONCURRENT_QUEUE_H_ */
