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
#include <optional>

class FairMQDevice;
class FairMQChannel;

namespace o2::DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameReadoutBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameReadoutBuilder
{
 public:
  SubTimeFrameReadoutBuilder() = delete;
  SubTimeFrameReadoutBuilder(MemoryResources &pMemRes, bool pDplEnabled);

  bool addHbFrames(const o2::header::DataOrigin &pDataOrig,
    const o2::header::DataHeader::SubSpecificationType pSubSpecification,
    const ReadoutSubTimeframeHeader& pHdr,
    std::vector<FairMQMessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen);


  bool addEquipmentData(const o2::header::DataOrigin &pDataOrig,
    const o2::header::DataHeader::SubSpecificationType pSubSpecification,
    const ReadoutSubTimeframeHeader& pHdr,
    std::vector<FairMQMessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen);

  std::optional<std::uint32_t> getCurrentStfId() const {
    return (mStf) ? std::optional<std::uint32_t>(mStf->header().mId) : std::nullopt;
  }

  std::optional<std::unique_ptr<SubTimeFrame>> getStf() {

    std::unique_ptr<SubTimeFrame> lStf = std::move(mStf);

    // mark as NULL if data was rejected
    if (lStf && !mAcceptStfData) {
      lStf->setOrigin(SubTimeFrame::Header::Origin::eNull);
    }

    mStf = nullptr;
    mAcceptStfData = true;
    mFirstFiltered.clear();

    return (lStf) ? std::optional<std::unique_ptr<SubTimeFrame>>(std::move(lStf)) : std::nullopt;
  }

  inline void stop() {
    mRunning = false;
    mMemRes.stop();
  }

 private:
  bool mRunning = true;

  std::unique_ptr<SubTimeFrame> mStf;
  bool mAcceptStfData = true;        // toggle on allocation issues

  // filtering: keep info if the first HBFrame is already kept back
  std::unordered_map<o2::header::DataHeader::SubSpecificationType, bool> mFirstFiltered;

  bool mDplEnabled;

  MemoryResources &mMemRes;
};


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameFileBuilder
{
 public:
  SubTimeFrameFileBuilder() = delete;
  SubTimeFrameFileBuilder(MemoryResources &pMemRes, const std::size_t pDataSegSize,
    const std::size_t pHdrSegSize, bool pDplEnabled);

  void adaptHeaders(SubTimeFrame *pStf);

  // allocate appropriate message for the header
  inline
  FairMQMessagePtr newHeaderMessage(const o2::header::Stack &pIncomingStack, const std::uint64_t pTfId) {
    std::unique_ptr<FairMQMessage> lMsg;

    if (mDplEnabled) {
      auto lStack = o2::header::Stack(
        pIncomingStack,
        o2::framework::DataProcessingHeader{pTfId}
      );

      lMsg = mMemRes.newHeaderMessage(reinterpret_cast<char*>(lStack.data()), lStack.size());
      if (!lMsg) {
        return nullptr;
      }
    } else {
      lMsg = mMemRes.newHeaderMessage(reinterpret_cast<char*>(pIncomingStack.data()), pIncomingStack.size());
      if (!lMsg) {
        return nullptr;
      }
    }

    return lMsg;
  }

  // allocate appropriate message for the data blocks
  inline
  FairMQMessagePtr newDataMessage(const std::size_t pSize) {
    return mMemRes.newDataMessage(pSize);
  }

  void stop() {
    mMemRes.stop();
  }

 private:
  MemoryResources &mMemRes;

  bool mDplEnabled;
};

////////////////////////////////////////////////////////////////////////////////
/// TimeFrameBuilder
////////////////////////////////////////////////////////////////////////////////

class TimeFrameBuilder
{
 public:
  TimeFrameBuilder() = delete;
  TimeFrameBuilder(SyncMemoryResources &pMemRes, bool pDplEnabled);

  // make allocate the memory here
  void allocate_memory(const std::size_t pDataSegSize, const std::size_t pHdrSegSize);

  void adaptHeaders(SubTimeFrame *pStf);


  FairMQMessagePtr newHeaderMessage(const char *pData, const std::size_t pSize);

  inline
  FairMQMessagePtr newDataMessage(const std::size_t pSize) {
    return mMemRes.newDataMessage(pSize);
  }

  inline
  FairMQMessagePtr newDataMessage(const char *pData, const std::size_t pSize) {
    return mMemRes.newDataMessage(pData, pSize);
  }

  inline
  void newDataMessages(const std::vector<FairMQMessagePtr> &pSrcMsgs, std::vector<FairMQMessagePtr> &pDstMsgs) {
    mMemRes.newDataMessages(pSrcMsgs, pDstMsgs);
  }

  inline void stop() {
    mMemRes.stop();
  }

  inline auto freeData() const { return mMemRes.freeData(); }

 private:

  bool mDplEnabled;

  SyncMemoryResources &mMemRes;
};

} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_BUILDER_H_ */
