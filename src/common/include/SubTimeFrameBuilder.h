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

#ifndef ALICEO2_SUBTIMEFRAME_BUILDER_H_
#define ALICEO2_SUBTIMEFRAME_BUILDER_H_

#include "SubTimeFrameDataModel.h"
#include "MemoryUtils.h"

#include <Headers/DataHeader.h>
#include <Headers/Stack.h>
#include <Framework/DataProcessingHeader.h>

#include <vector>
#include <mutex>

class FairMQDevice;
class FairMQChannel;

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameReadoutBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameReadoutBuilder
{
 public:
  SubTimeFrameReadoutBuilder() = delete;
  SubTimeFrameReadoutBuilder(FairMQChannel& pChan, bool pDplEnabled);

  void addHbFrames(const o2::header::DataOrigin &pDataOrig,
    const o2::header::DataHeader::SubSpecificationType pSubSpecification,
    ReadoutSubTimeframeHeader& pHdr,
    std::vector<FairMQMessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen);
  std::unique_ptr<SubTimeFrame> getStf();

 private:

  std::unique_ptr<SubTimeFrame> mStf;

  // filtering: keep info if the first HBFrame is already kept back
  std::unordered_map<o2::header::DataHeader::SubSpecificationType, bool> mFirstFiltered;

  bool mDplEnabled;

  std::unique_ptr<FMQUnsynchronizedPoolMemoryResource> mHeaderMemRes;
};


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameFileBuilder
{
 public:
  SubTimeFrameFileBuilder() = delete;
  SubTimeFrameFileBuilder(FairMQChannel& pChan, const std::size_t pDataSegSize, bool pDplEnabled);

  void adaptHeaders(SubTimeFrame *pStf);

  FairMQMessagePtr getDataMessage(const std::size_t pSize) {
    return mDataMemRes->NewFairMQMessage(pSize);
  }

  // allocate appropriate message for the header
  FairMQMessagePtr getHeaderMessage(const o2::header::Stack &pIncomingStack, const std::uint64_t pTfId) {
    std::unique_ptr<FairMQMessage> lMsg;

    if (mDplEnabled) {
      auto lStack = o2::header::Stack(
        pIncomingStack,
        o2::framework::DataProcessingHeader{pTfId}
      );

      lMsg = mHeaderMemRes->NewFairMQMessage(lStack.size());
      std::memcpy(lMsg->GetData(), lStack.data(), lStack.size());

    } else {
      lMsg = mHeaderMemRes->NewFairMQMessage(pIncomingStack.size());
      std::memcpy(lMsg->GetData(), pIncomingStack.data(), pIncomingStack.size());
    }

    return lMsg;
  }

  auto& getHeaderMemRes() const { return *mHeaderMemRes; }

 private:

  bool mDplEnabled;

  std::unique_ptr<RegionAllocatorResource<alignof(o2::header::DataHeader)>> mHeaderMemRes;
  std::unique_ptr<RegionAllocatorResource<>> mDataMemRes;
};

////////////////////////////////////////////////////////////////////////////////
/// TimeFrameBuilder
////////////////////////////////////////////////////////////////////////////////

class TimeFrameBuilder
{
 public:
  TimeFrameBuilder() = delete;
  TimeFrameBuilder(FairMQChannel& pChan, const std::size_t pDataSegSize, bool pDplEnabled);

  void adaptHeaders(SubTimeFrame *pStf);

  FairMQMessagePtr getNewHeaderMessage(const std::size_t pSize) {
    return mHeaderMemRes->NewFairMQMessage(pSize);
  }

  FairMQMessagePtr getNewDataMessage(const std::size_t pSize) {
    return mDataMemRes->NewFairMQMessage(pSize);
  }

 private:

  bool mDplEnabled;
  FairMQChannel &mOutputChan;

  std::unique_ptr<RegionAllocatorResource<alignof(o2::header::DataHeader)>> mHeaderMemRes;
  std::unique_ptr<RegionAllocatorResource<>> mDataMemRes;
};

}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_BUILDER_H_ */
