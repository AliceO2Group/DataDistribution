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

#ifndef TF_BUILDER_INPUT_DEFS_H_
#define TF_BUILDER_INPUT_DEFS_H_

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>

#include <chrono>
#include <vector>
#include <memory>

namespace o2::DataDistribution
{

enum InputRunState { CONFIGURING, RUNNING, TERMINATED };

struct ReceivedStfMeta {
    enum MetaType { ADD, DELETE, INFO } mType;
    TimeFrameIdType mStfId;
    SubTimeFrame::Header::Origin mStfOrigin;
    std::chrono::time_point<std::chrono::steady_clock> mTimeReceived;

    FairMQMessagePtr mRecvStfHeaderMeta;
    std::unique_ptr<std::vector<FairMQMessagePtr>> mRecvStfdata;
    std::unique_ptr<SubTimeFrame> mStf;
    std::string mStfSenderId;

    ReceivedStfMeta(MetaType pType, const TimeFrameIdType pStfId)
    : mType(pType),
      mStfId(pStfId)
    { }

    ReceivedStfMeta(const TimeFrameIdType pStfId,
                    const SubTimeFrame::Header::Origin pStfOrigin,
                    const std::string &pStfSenderId,
                    FairMQMessagePtr &&pRcvHdrMsg,
                    std::unique_ptr<std::vector<FairMQMessagePtr>> &&pRecvStfdata)
    : mType(INFO),
      mStfId(pStfId),
      mStfOrigin(pStfOrigin),
      mTimeReceived(std::chrono::steady_clock::now()),
      mRecvStfHeaderMeta(std::move(pRcvHdrMsg)),
      mRecvStfdata(std::move(pRecvStfdata)),
      mStf(nullptr),
      mStfSenderId(pStfSenderId)
    { }
};

} /* namespace o2::DataDistribution */

#endif /* TF_BUILDER_INPUT_DEFS_H_ */
