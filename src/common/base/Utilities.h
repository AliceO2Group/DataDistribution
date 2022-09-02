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

#include <type_traits>
#include <memory>
#include <thread>
#include <functional>

#include <array>
#include <numeric>
#include <chrono>
#include <type_traits>

#include <boost/dynamic_bitset.hpp>

namespace o2::DataDistribution
{

static inline constexpr
void assume(const bool pPred) {
  if (!pPred) {
    __builtin_unreachable();
  }
}

template<std::size_t ALIGN>
std::size_t align_size_up(const std::size_t pSize) {
  return (pSize + ALIGN - 1) / ALIGN * ALIGN;
}

// helper function to get default LHC period
static inline
std::string getDefaultLhcPeriod() {
  // From O2 CommonServices::datatakingContextSpec()
  static const char* months[12] = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};
  time_t now = time(nullptr);
  auto ltm = gmtime(&now);
  return std::string(months[ltm->tm_mon]);
}

// convert enum to numeric value
template <typename T>
constexpr auto operator+(const T p) noexcept -> std::enable_if_t<std::is_enum<T>::value, std::underlying_type_t<T>>
{
  return static_cast<std::underlying_type_t<T>>(p);
}

// create threads using a object method
template <class F, class ... Args>
std::thread create_thread_member(const char* name, F&& f, Args&&... args) {
  char *lName = strdup(name);
  return std::thread([=]{
#if defined(__linux__)
    pthread_setname_np(pthread_self(), lName);
#endif
    // run the function
    auto fun = std::mem_fn(f);
    fun(args...);

#if defined(__linux__)
    free((void*)lName);
#endif
  });
}

// return duration in desired units
template <
    class unit       = std::chrono::seconds,
    class clock_t    = std::chrono::steady_clock,
    class duration_t = std::chrono::microseconds >
double since(std::chrono::time_point<clock_t, duration_t> const& start)
{
  if constexpr (std::is_same_v<unit, std::chrono::seconds>) {
    return std::chrono::duration<double>(clock_t::now() - start).count();
  } else {
    return std::chrono::duration<double>(clock_t::now() - start) / unit(1);
  }
}

// return duration in desired units, with provided finish
template <
    class unit       = std::chrono::seconds,
    class clock_t    = std::chrono::steady_clock,
    class duration_t = std::chrono::microseconds >
double since(std::chrono::time_point<clock_t, duration_t> const& start, std::chrono::time_point<clock_t, duration_t> const& finish)
{
  if constexpr (std::is_same_v<unit, std::chrono::seconds>) {
    return std::chrono::duration<double>(finish - start).count();
  } else {
    return std::chrono::duration<double>(finish - start) / unit(1);
  }
}

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
  void clear() {
    mCount = 0;
    mIndex = 0;
  }

private:
  std::array<T, N> mSamples;
  std::size_t mIndex = 0;
  std::size_t mCount = 0;
};


class EventRecorder {
public:
  EventRecorder() = delete;
  EventRecorder(const std::size_t pInitSize) {
    mRecords.resize(pInitSize);
  }

  inline
  bool SetEvent(const std::uint64_t pEvt, const bool pVal = true) {
    bool lRet = false;
    if (mFirstEvent == std::uint64_t(-1)) {
      mFirstEvent = pEvt;
    }

    if (pEvt < mFirstEvent) {
      return false;
    }

    const auto lIdx = pEvt - mFirstEvent;
    manageSize(lIdx);

    lRet = mRecords[lIdx];
    mRecords[lIdx] = pVal;

    return (lRet != pVal);
  }

  inline
  bool GetEvent(const std::uint64_t pEvt) {
    if (mFirstEvent == std::uint64_t(-1)) {
      return false; // no recorded events
    }
    if (pEvt < mFirstEvent) {
      return false; // not recorded
    }
    const auto lIdx = pEvt - mFirstEvent;
    if (mRecords.size() <= lIdx) {
      return false; // beyond records
    }

    return mRecords[lIdx];
  }

  void reset() {
    mFirstEvent = std::uint64_t(-1);
    mRecords.reset();
  }

private:
  std::uint64_t mFirstEvent = std::uint64_t(-1);
  boost::dynamic_bitset<std::uint64_t> mRecords;

  inline
  void manageSize(const std::uint64_t pIdx) {
    if (mRecords.size() <= pIdx) {
      const auto lNewSize = (pIdx + 3) * 4 / 3;
      mRecords.resize(lNewSize);
    }
  }
};

// Compact, fixed-size, and efficient bitfield data type
// Bit indexing is 1-based.
template<unsigned NUM_TOKENS>
struct TokenBitfield {
  using TokenBitfieldElemType = unsigned long; // used for __builtin_* functions
  using TokenBitfieldIndexType = std::size_t;

  static constexpr TokenBitfieldIndexType sInvalidIdx = TokenBitfieldIndexType(0);
  static constexpr const unsigned NUM_ELEM_BITS = sizeof (TokenBitfieldElemType) * 8;
  static constexpr const unsigned NUM_ELEMS = (NUM_TOKENS + NUM_ELEM_BITS - 1) / NUM_ELEM_BITS;

  TokenBitfieldElemType mRequestTokenBitset[NUM_ELEMS];

  TokenBitfield() {
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      mRequestTokenBitset[i] = 0;
    }
  }

  TokenBitfield(const TokenBitfield &a) = default;

  inline void set(TokenBitfieldIndexType idx) {
    assert ((idx > 0) && (idx <= NUM_TOKENS));
    idx -= 1;

    const auto elem = idx / NUM_ELEM_BITS;
    idx = idx % NUM_ELEM_BITS;

    assert (idx < NUM_ELEM_BITS);

    const auto mask = TokenBitfieldElemType(1) << (idx);
    mRequestTokenBitset[elem] |= mask;
  }

  inline void set_all() {
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      mRequestTokenBitset[i] = ~TokenBitfieldElemType(0);
    }
  }

  inline void clear_all() {
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      mRequestTokenBitset[i] = TokenBitfieldElemType(0);
    }
  }

  inline void clr(TokenBitfieldIndexType idx) {
    assert ((idx > 0) && (idx <= NUM_TOKENS));
    idx -= 1;

    const auto elem = idx / NUM_ELEM_BITS;
    idx = idx % NUM_ELEM_BITS;

    assert (idx < NUM_ELEM_BITS);

    const auto mask = ~(TokenBitfieldElemType(1) << idx);
    mRequestTokenBitset[elem] &= mask;
  }

  inline bool get(TokenBitfieldIndexType idx) const {
    assert ((idx > 0) && (idx <= NUM_TOKENS));
    idx -= 1;

    const auto elem = idx / NUM_ELEM_BITS;
    idx = idx % NUM_ELEM_BITS;

    assert (idx < NUM_ELEM_BITS);

    const auto mask = TokenBitfieldElemType(1) << idx;
    return (mRequestTokenBitset[elem] & mask);
  }

  inline TokenBitfieldIndexType popcnt() const {
    TokenBitfieldIndexType lRet = 0;
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      lRet += __builtin_popcountl(mRequestTokenBitset[i]);
    }
    return lRet;
  }

  inline bool empty() const {
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      if (mRequestTokenBitset[i]) {
        return false;
      }
    }
    return true;
  }

  inline constexpr static std::size_t size() {
    return NUM_TOKENS;
  }

  inline TokenBitfieldIndexType first() const {
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      if (TokenBitfieldIndexType lRet = __builtin_ffsl(mRequestTokenBitset[i]); lRet) {
        return ((i * NUM_ELEM_BITS) + lRet);
      }
    }
    return sInvalidIdx;
  }

  inline TokenBitfieldIndexType random_idx() const {
    std::uint8_t lIdxes[NUM_TOKENS] = { 0 };
    std::size_t lIdxCnt = 0;

    for (TokenBitfieldIndexType i = 1; i <= NUM_TOKENS; i++) {
      if (get(i)) {
        lIdxes[lIdxCnt++] = i;
      }
    }

    return (lIdxCnt == 0) ? sInvalidIdx : lIdxes[rand() % lIdxCnt];
  }

  inline TokenBitfield& operator&=(const TokenBitfield& b) {
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      mRequestTokenBitset[i] &= b.mRequestTokenBitset[i];
    }
    return *this;
  }

  inline friend TokenBitfield operator&(const TokenBitfield& a, const TokenBitfield& b) {
    TokenBitfield r = a;
    r &= b;
    return r;
  }

  inline TokenBitfield& operator|=(const TokenBitfield& b) {
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      mRequestTokenBitset[i] |= b.mRequestTokenBitset[i];
    }
    return *this;
  }

  inline TokenBitfield operator~() const {
    TokenBitfield lRet = *this;

    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      lRet.mRequestTokenBitset[i] = ~(lRet.mRequestTokenBitset[i]);
    }
    return lRet;
  }

  inline operator bool() const {
    bool lNotEmpty = false;
    for (unsigned i = 0; i < NUM_ELEMS; i++) {
      lNotEmpty |= !!(mRequestTokenBitset[i]);
    }
    return lNotEmpty;
  }
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_UTILITIES_H_ */
