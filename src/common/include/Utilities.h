// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#ifndef ALICEO2_DATADIST_UTILITIES_H_
#define ALICEO2_DATADIST_UTILITIES_H_

#include <O2Device/O2Device.h>

#include <type_traits>
#include <memory>

#include <vector>
#include <numeric>

namespace o2
{
namespace DataDistribution
{

template <
  typename T,
  size_t N = 1024,
  typename = typename std::enable_if<std::is_arithmetic<T>::value && (N > 0), T>::type>
class RunningSamples
{
 public:
  RunningSamples()
    : mSamples(N, T(0)),
      mIndex(0),
      mCount(0)
  {
  }
  RunningSamples(const T& pInitVal)
    : mSamples(N, pInitVal),
      mIndex(0),
      mCount(0)
  {
  }

  void Fill(const T& pNewVal)
  {
    mSamples[mIndex] = pNewVal;
    mIndex = (mIndex + 1) % N;
    mCount = std::min(mCount + 1, N);
  }

  T Sum() const
  {
    return std::accumulate(begin(), end(), T(0));
  }

  T Mean() const
  {
    return Sum() / T(mCount);
  }

  std::pair<T, T> MinMax() const
  {
    if (mCount == 0)
      return { T(0), T(0) };

    auto[min, max] = std::minmax_element(begin(), end());
    return { *min, *max };
  }

  const auto begin() const
  {
    return mSamples.begin();
  }
  const auto end() const
  {
    return mSamples.begin() + mCount;
  }

 private:
  std::vector<T> mSamples;
  std::size_t mIndex = 0;
  std::size_t mCount = 0;
};

#if 0 // Deprecate in favour of FairMQ channel allocators

using namespace o2::Base;

static constexpr std::uintptr_t gChanPtrAlign = 8;
// static constexpr std::uintptr_t gChanPtrAlign = alignof(std::max_align_t);

#define ASSERT(left, operator, right)                                                                                                                                                            \
  {                                                                                                                                                                                              \
    if (!((left) operator(right))) {                                                                                                                                                             \
      std::cerr << "ASSERT FAILED: " << #left << #operator<< #right << " @ " << __FILE__ << " (" << __LINE__ << "). " << #left << "=" <<(left) << "; " << #right << "=" << (right) << std::endl; \
    }                                                                                                                                                                                            \
  }

class ChannelAllocator;

template <class T>
class ChannelPtr
{
  friend class ChannelAllocator;

 public:
  typedef T element_type;
  typedef T value_type;
  typedef T* pointer;
  typedef std::ptrdiff_t difference_type;
  typedef typename std::add_lvalue_reference<element_type>::type reference;

  ChannelPtr() = default;
  ~ChannelPtr() = default;

  explicit ChannelPtr(const ChannelPtr&) = delete;
  ChannelPtr& operator=(const ChannelPtr&) = delete;

  ChannelPtr(ChannelPtr&&) = default;
  ChannelPtr& operator=(ChannelPtr&& a) = default;

  ChannelPtr& operator=(FairMQMessagePtr&& a) noexcept
  {
    const auto lAddr = reinterpret_cast<std::uintptr_t>(a->GetData());
    ASSERT((lAddr & (gChanPtrAlign - 1)), ==, 0);

    mMessage = std::move(a);
    return *this;
  }

  void reset(ChannelPtr& p)
  {
    mMessage = std::move(p);
  }

  void swap(ChannelPtr& p) noexcept
  {
    mMessage.swap(p.mMessage);
  }

  pointer get() const noexcept
  {
    const auto lAddr = reinterpret_cast<std::uintptr_t>(mMessage->GetData());
    ASSERT((lAddr & (gChanPtrAlign - 1)), ==, 0);
    return reinterpret_cast<pointer>(lAddr);
  }

  pointer release() noexcept
  {
    pointer lPtr = get();
    std::exchange(mMessage, nullptr);
    return lPtr;
  }

  reference operator*() const noexcept
  {
    assert(get() != nullptr);
    return *get();
  }

  pointer operator->() const noexcept
  {
    assert(get() != nullptr);
    return get();
  }

  explicit operator bool() const noexcept
  {
    return mMessage != nullptr;
  }

  operator void*() const noexcept
  {
    return reinterpret_cast<void*>(get());
  }

  FairMQMessagePtr getMessage()
  {
    return std::exchange(mMessage, nullptr);
  }

 private:
  ChannelPtr(FairMQMessagePtr& m) : mMessage(std::move(m))
  {
  }
  FairMQMessagePtr mMessage = nullptr;
};

class ChannelAllocator
{
 public:
  static ChannelAllocator& get()
  {
    static ChannelAllocator sInstance;
    return sInstance;
  }

  void addChannel(const int pChannId, FairMQChannel& pChan)
  {
    mChannels.emplace(pChannId, pChan);
  }

  template <class T>
  ChannelPtr<T> allocate(const int pChannId)
  {
    assert(mChannels.count(pChannId) == 1);
    FairMQChannel& lChan = mChannels.at(pChannId);
    FairMQMessagePtr lMessage = lChan.NewMessage(sizeof(T));
      // lChan.NewMessage(sizeof(T) + gChanPtrAlign - 1)

    ChannelPtr<T> lChanPtr(lMessage);

    std::memset(lChanPtr.get(), 0, sizeof(T));
    return lChanPtr;
  }

 private:
  ChannelAllocator() = default;
  ChannelAllocator(ChannelAllocator&) = delete;
  ChannelAllocator operator=(ChannelAllocator&) = delete;

  std::map<int, FairMQChannel&> mChannels;
};

template <class T, class... Args>
inline ChannelPtr<T> make_channel_ptr(const int pChannId, Args&&... args)
{
  ChannelPtr<T> lPtr(std::move(ChannelAllocator::get().allocate<T>(pChannId)));
  assert(lPtr.get() != nullptr);
  new (lPtr.get()) T(std::forward<Args>(args)...);
  return lPtr;
}

#endif
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_UTILITIES_H_ */
