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

#ifndef STF_SENDER_OUTPUT_DEFS_H_
#define STF_SENDER_OUTPUT_DEFS_H_

#include <mutex>
#include <chrono>
#include <memory>

namespace o2::DataDistribution
{

class SubTimeFrame;

enum ConnectStatus { eOK, eEXISTS, eCONNERR };


struct StdSenderOutputCounters {
  std::mutex mCountersLock;

  struct Values {
    std::uint32_t mSchedulerStfRejectedCnt = 0;

    // buffer state
    struct {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
    } mBuffered;
    // buffered in sending
    struct {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
    } mInSending;

    // total sent
    struct {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
      std::uint32_t mMissing = 0;
    } mTotalSent;
  } mValues;
};

struct ScheduledStfInfo {
  std::unique_ptr<SubTimeFrame>         mStf;
  std::chrono::steady_clock::time_point mTimeAdded;
  std::chrono::steady_clock::time_point mTimeRequested;
};

} /* namespace o2::DataDistribution */

#endif /* STF_SENDER_OUTPUT_DEFS_H_ */
