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

#ifndef STF_SENDER_OUTPUT_DEFS_H_
#define STF_SENDER_OUTPUT_DEFS_H_

#include <mutex>


namespace o2::DataDistribution
{

enum ConnectStatus { eOK, eEXISTS, eCONNERR };


struct StdSenderOutputCounters {
  std::mutex mCountersLock;

  struct Values {
    // buffer state
    struct alignas(128) {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
    } mBuffered;
    // buffered in sending
    struct alignas(128) {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
    } mInSending;

    // total sent
    struct alignas(128) {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
      std::uint32_t mMissing = 0;
    } mTotalSent;
  } mValues;
};

} /* namespace o2::DataDistribution */

#endif /* STF_SENDER_OUTPUT_DEFS_H_ */
