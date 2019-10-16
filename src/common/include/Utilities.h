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

#include <vector>
#include <numeric>

namespace o2
{
namespace DataDistribution
{

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
};

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
    return mCount == 0 ? T(0) : (Sum() / T(mCount));
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

}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_UTILITIES_H_ */
