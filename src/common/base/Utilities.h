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

#ifndef ALICEO2_DATADIST_UTILITIES_H_
#define ALICEO2_DATADIST_UTILITIES_H_

#include <fairmq/FairMQDevice.h>

#include <type_traits>
#include <memory>
#include <thread>
#include <functional>

#include <array>
#include <numeric>

namespace o2
{
namespace DataDistribution
{

template <class F, class ... Args>
std::thread create_thread_member(const char* name, F&& f, Args&&... args) {
  return std::thread([=]{
#if defined(__linux__)
    pthread_setname_np(pthread_self(), name);
#endif
    auto fun = std::mem_fn(f);
    fun(args...);
  });
}

class DataDistDevice : public FairMQDevice {

public:

  void WaitForRunningState() const {
    while (GetCurrentState() < fair::mq::State::Running) {
      using namespace std::chrono_literals;
      std::this_thread::sleep_for(20ms);
    }
  }

  bool IsRunningState() const {
    return (GetCurrentState() == fair::mq::State::Running);
  }

  bool IsReadyOrRunningState() const {
    return ((GetCurrentState() == fair::mq::State::Running) || (GetCurrentState() == fair::mq::State::Ready));
  }
};

template <
  typename T,
  size_t N = 1024,
  typename = typename std::enable_if<std::is_arithmetic<T>::value && (N > 0), T>::type>
class RunningSamples
{
 public:
  RunningSamples()
    : mSamples(),
      mIndex(0),
      mCount(0)
  {
    mSamples.fill(T(0));
  }
  RunningSamples(const T& pInitVal)
    : mSamples(),
      mIndex(0),
      mCount(0)
  {
    mSamples.fill(pInitVal);
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
    return mCount == 0 ? T(0) : (Sum() / T(mCount));
  }

  T MeanStep() const
  {
    T lStepSum = 0;
    std::size_t lStepsCounted = 0;

    if (mCount < 2) {
      return T(0);
    }

    for (auto it = begin()+1; it < end(); it++) {
      if (*it <= *(it-1)) {
        continue;
      }

      lStepSum += (*it - *(it-1));
      lStepsCounted++;
    }

    return (lStepsCounted > 0) ? ((T)lStepSum / (T)lStepsCounted) : T(0);
  }

  T MeanStepFreq() const
  {
    const T lMeanStep = MeanStep();
    return (lMeanStep != T(0)) ? (T(1) / lMeanStep) : T(0);
  }

  std::pair<T, T> MinMax() const
  {
    if (mCount == 0)
      return { T(0), T(0) };

    auto[min, max] = std::minmax_element(begin(), end());
    return { *min, *max };
  }

  auto begin() const
  {
    return mSamples.begin();
  }
  auto end() const
  {
    return mSamples.begin() + mCount;
  }

 private:
  std::array<T, N> mSamples;
  std::size_t mIndex = 0;
  std::size_t mCount = 0;
};

}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_UTILITIES_H_ */
