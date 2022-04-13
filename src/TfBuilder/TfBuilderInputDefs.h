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

#ifndef TF_BUILDER_INPUT_DEFS_H_
#define TF_BUILDER_INPUT_DEFS_H_

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>

#include <chrono>
#include <vector>
#include <memory>

namespace o2::DataDistribution
{
class IovStfHdrMeta;

enum InputRunState { CONFIGURING, RUNNING, TERMINATED };

struct ReceivedStfMeta {
    enum MetaType { ADD, DELETE, INFO } mType;
    TimeFrameIdType mStfId;
    SubTimeFrame::Header::Origin mStfOrigin;
    std::chrono::time_point<std::chrono::steady_clock> mTimeReceived;

    std::unique_ptr<IovStfHdrMeta> mRecvStfHeaderMeta;
    std::unique_ptr<std::vector<fair::mq::MessagePtr>> mRecvStfdata;
    std::unique_ptr<SubTimeFrame> mStf;
    std::string mStfSenderId;

    ReceivedStfMeta(MetaType pType, const TimeFrameIdType pStfId)
    : mType(pType),
      mStfId(pStfId)
    { }

    ReceivedStfMeta(const TimeFrameIdType pStfId,
                    const SubTimeFrame::Header::Origin pStfOrigin,
                    const std::string &pStfSenderId,
                    std::unique_ptr<IovStfHdrMeta> &&pRcvHdrMeta,
                    std::unique_ptr<std::vector<fair::mq::MessagePtr>> &&pRecvStfdata)
    : mType(INFO),
      mStfId(pStfId),
      mStfOrigin(pStfOrigin),
      mTimeReceived(std::chrono::steady_clock::now()),
      mRecvStfHeaderMeta(std::move(pRcvHdrMeta)),
      mRecvStfData(std::move(pRecvStfData)),
      mStf(nullptr),
      mStfSenderId(pStfSenderId)
    { }
};

} /* namespace o2::DataDistribution */

#endif /* TF_BUILDER_INPUT_DEFS_H_ */
