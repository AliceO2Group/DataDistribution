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

#include "SubTimeFrameBuilder.h"
#include "ReadoutDataModel.h"
#include "MemoryUtils.h"
#include "DataDistLogger.h"

#include <Headers/DataHeader.h>
#include <Headers/Stack.h>
#include <Framework/DataProcessingHeader.h>

#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQUnmanagedRegion.h>

#include <optional>

namespace o2::DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameReadoutBuilder
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameReadoutBuilder::SubTimeFrameReadoutBuilder(SyncMemoryResources &pMemRes)
  : mStf(nullptr),
    mMemRes(pMemRes)
{
  mMemRes.mHeaderMemRes = std::make_unique<HeaderRegionAllocatorResource>(
    "O2HeadersRegion",
    std::nullopt, std::size_t(128) << 20,
    *mMemRes.mShmTransport,
    0, /* region flags ? */
    true /* Header alloc can fail with large FLP-DPL backpreassure  */
  );

  mMemRes.start();
}

bool SubTimeFrameReadoutBuilder::addHbFrames(
  const o2::header::DataOrigin &pDataOrig,
  const o2::header::DataHeader::SubSpecificationType pSubSpecification,
  const ReadoutSubTimeframeHeader& pHdr,
  std::vector<FairMQMessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen)
{
  static thread_local std::vector<bool> lRemoveBlocks;

  if (!mRunning) {
    WDDLOG("Adding HBFrames while STFBuilder is not running!");
    mAcceptStfData = false;
    return false;
  }

  if (!mAcceptStfData) {
    return false;
  }

  if (!mStf) {
    mStf = std::make_unique<SubTimeFrame>(pHdr.mTimeFrameId);
    mStf->updateRunNumber(pHdr.mRunNumber);

    mAcceptStfData = true;
    mFirstFiltered.clear();
  }

  mStf->updateFirstOrbit(pHdr.mTimeframeOrbitFirst);
  if (pHdr.mTimeframeOrbitFirst == 0) {
    WDDLOG_ONCE("READOUT INTERFACE: First orbit in TF is set to 0.");
  }

  // filter empty trigger
  lRemoveBlocks.clear();
  lRemoveBlocks.resize(pHBFrameLen);
  {
    if (ReadoutDataUtils::sEmptyTriggerHBFrameFilterring) {
      // filter empty trigger start stop pages
      for (std::size_t i = 0; i < pHBFrameLen; i++) {
        if (lRemoveBlocks[i]) {
          continue; // already discarded
        }

        if (i == 0) {
          // NOTE: this can be implemented by checking trigger flags in the RDH for the TF bit
          //       Perhaps switch to that method later, when the RHD is more stable
          //       Fow now, we simply keep the first HBFrame of each equipment in the STF
          const auto R = RDHReader(pHbFramesBegin[0]);
          const auto lSubSpec = ReadoutDataUtils::getSubSpecification(R);
          if (!mFirstFiltered[lSubSpec]) {
            mFirstFiltered[lSubSpec] = true;
            continue; // we keep the first HBFrame for each subspec (equipment)
          }
        }

        if (!ReadoutDataUtils::filterEmptyTriggerBlocks(
              reinterpret_cast<const char*>(pHbFramesBegin[i]->GetData()),
              pHbFramesBegin[i]->GetSize()) ) {
          lRemoveBlocks[i] = false;
        } else {
          lRemoveBlocks[i] = true;
        }
      }
    }
  }

  // sanity check
  {
    if (ReadoutDataUtils::sRdhSanityCheckMode != ReadoutDataUtils::eNoSanityCheck) {
      // check blocks individually
      for (std::size_t i = 0; i < pHBFrameLen; i++) {

        if (lRemoveBlocks[i]) {
          continue; // already filtered out
        }

        const auto lOk = ReadoutDataUtils::rdhSanityCheck(
          reinterpret_cast<const char*>(pHbFramesBegin[i]->GetData()),
          pHbFramesBegin[i]->GetSize());

        if (!lOk && (ReadoutDataUtils::sRdhSanityCheckMode == ReadoutDataUtils::eSanityCheckDrop)) {
          WDDLOG("RDH SANITY CHECK: Removing data block");

          lRemoveBlocks[i] = true;

        } else if (!lOk && (ReadoutDataUtils::sRdhSanityCheckMode == ReadoutDataUtils::eSanityCheckPrint)) {

          IDDLOG("Printing data blocks of update with TF ID={} Lik ID={}",
            pHdr.mTimeFrameId, unsigned(pHdr.mLinkId));

          // dump the data block, skipping data
          std::size_t lCurrentDataIdx = 0;

          const auto Ri = RDHReader(pHbFramesBegin[i]);
          const auto lCru = Ri.getCruID();
          const auto lEp = Ri.getEndPointID();
          const auto lLink = Ri.getLinkID();

          while (lCurrentDataIdx < pHbFramesBegin[i]->GetSize()) {
            const auto lDataSizeLeft = std::size_t(pHbFramesBegin[i]->GetSize()) - lCurrentDataIdx;

            std::string lInfoStr = "RDH block (64 bytes in total) of [" + std::to_string(i) + "] 8 kiB page";

            o2::header::hexDump(lInfoStr.c_str(),
              reinterpret_cast<char*>(pHbFramesBegin[i]->GetData()) + lCurrentDataIdx,
              std::size_t(std::min(std::size_t(64), lDataSizeLeft)));

            IDDLOG("RDH info CRU={} Endpoint={} Link={}", lCru, lEp, lLink);

            const auto R = RDHReader(
              reinterpret_cast<const char*>(pHbFramesBegin[i]->GetData()) + lCurrentDataIdx,
              lDataSizeLeft
            );
            lCurrentDataIdx += std::min(std::size_t(R.getOffsetToNext()), lDataSizeLeft);
          }
        }
      }
    }
  }

  assert(pHdr.mTimeFrameId == mStf->header().mId);

  DataHeader lDataHdr(
    o2::header::gDataDescriptionRawData,
    pDataOrig,
    pSubSpecification,
    0 /* Update later */
  );
  lDataHdr.payloadSerializationMethod = gSerializationMethodNone;

  const o2hdr::DataIdentifier lDataId(o2::header::gDataDescriptionRawData.str,  pDataOrig.str);

  // Speed up adding messages by saving the vector of split-payloads
  std::vector<FairMQMessagePtr> *pInVector = nullptr;

  for (size_t i = 0; i < pHBFrameLen; i++) {

    if (lRemoveBlocks[i]) {
      continue; // already filtered out
    }

    // we need at least one header per equipment
    if (!pInVector) {
      lDataHdr.payloadSize = pHbFramesBegin[i]->GetSize();

      auto lDplHdr = o2::framework::DataProcessingHeader{mStf->header().mId};
      lDplHdr.creation = mStf->header().mCreationTimeMs;
      auto lStack = Stack(lDataHdr, lDplHdr);

      auto lHdrMsg = mMemRes.newHeaderMessage(lStack.data(), lStack.size());
      if (!lHdrMsg) {
        WDDLOG_RL(1000, "Allocation error: dropping data of the current STF stf_id={}", pHdr.mRunNumber);

        // clear data of the partial STF
        mAcceptStfData = false;
        mStf->clear();

        return false;
      }

      pInVector = mStf->addStfDataReadout(lDataId, pSubSpecification, {std::move(lHdrMsg), std::move(pHbFramesBegin[i])});
    } else {
      // add the rest of messages
      assert (pInVector);
      mStf->addStfDataReadout(pInVector, std::move(pHbFramesBegin[i]));
    }
  }
  return true;
}

std::optional<std::unique_ptr<SubTimeFrame>> SubTimeFrameReadoutBuilder::addTopoStfData(
  const o2::header::DataOrigin &pDataOrig,
  const o2::header::DataHeader::SubSpecificationType pSubSpecification,
  const ReadoutSubTimeframeHeader& pHdr,
  std::vector<FairMQMessagePtr>::iterator &pHbFramesBegin, std::size_t &pHBFrameLen,
  const std::uint64_t pMaxNumMessages, bool pCutTfOnNewOrbit)
{
  static uint32_t sTfId = 1;

  auto isFirstPacketOfOrbit = [&](const FairMQMessagePtr &pMsg) -> bool {
    if (!pMsg) {
      return false;
    }
    try {
      const auto R = RDHReader(pMsg);
      if (R.getPageCounter() == 0) {
        return true;
      }
    } catch (...) {
      EDDLOG_RL(5000, "Error getting RDHReader instance. page_size={}", pMsg->GetSize());
    }
    return false;
  };

  if (!mRunning) {
    WDDLOG("Adding HBFrames while STFBuilder is not running!");
    return std::nullopt;
  }

  if (!mAcceptStfData || (pHBFrameLen == 0)) {
    return std::nullopt;
  }

  auto &lStftuple = mTopoStfMap[pSubSpecification];
  auto &lStfNumMessages = lStftuple.first;
  auto &lStf = lStftuple.second;

  if (!lStf) {
    lStf = std::make_unique<SubTimeFrame>(sTfId);
    lStfNumMessages = 0;
    sTfId++;

    lStf->updateRunNumber(pHdr.mRunNumber);
    if (pHdr.mTimeframeOrbitFirst != 0) {
      lStf->updateFirstOrbit(pHdr.mTimeframeOrbitFirst);
    }
  }

  auto lReturnStfIfFinished = [&](const auto &pHbFrame) -> std::optional<std::unique_ptr<o2::DataDistribution::SubTimeFrame> > {
    if (!lStf || (lStfNumMessages == 0)) {
      return std::nullopt;
    }
    // before starting, check if the existing STF is large enough and cut conditions are met
    if ((!pCutTfOnNewOrbit && (lStfNumMessages >= pMaxNumMessages)) || // orbit cut not used
          (pCutTfOnNewOrbit && (lStfNumMessages >= pMaxNumMessages) && isFirstPacketOfOrbit(pHbFrame))) { // orbit cut used

      lStf->setOrigin(SubTimeFrame::Header::Origin::eReadoutTopology);

      DDDLOG_RL(1000, "addTopoStfData: leaving and returning STF: numMessages={}", lStfNumMessages);
      lStfNumMessages = 0;

      mAcceptStfData = true;
      return std::optional<std::unique_ptr<SubTimeFrame>>(std::move(lStf));
    }

    return std::nullopt;
  };

  // check if we should cut at the first block
  {
    auto lDoneStfOpt = lReturnStfIfFinished(*pHbFramesBegin);
    if (lDoneStfOpt.has_value()) {
      return lDoneStfOpt;
    }
  }

  const o2hdr::DataIdentifier lDataId(o2::header::gDataDescriptionRawData.str,  pDataOrig.str);

  if (!lStf->stfDataExists(lDataId, pSubSpecification)) {
    DataHeader lDataHdr(
      o2::header::gDataDescriptionRawData,
      pDataOrig,
      pSubSpecification,
      0 /* Update later */
    );
    lDataHdr.payloadSerializationMethod = gSerializationMethodNone;
    lDataHdr.payloadSize = (*pHbFramesBegin)->GetSize();

    auto lDplHdr = o2::framework::DataProcessingHeader{lStf->header().mId};
    lDplHdr.creation = lStf->header().mCreationTimeMs;

    auto lStack = Stack(lDataHdr, lDplHdr);

    auto lHdrMsg = mMemRes.newHeaderMessage(lStack.data(), lStack.size());
    if (!lHdrMsg) {
      WDDLOG_RL(10000, "Allocation error: dropping data of the current STF stf_id={}", pHdr.mRunNumber);
      // clear data of the partial STF
      mAcceptStfData = false;
      lStf->clear();
      return std::nullopt;
    }

    lStf->addStfDataReadout(lDataId, pSubSpecification, SubTimeFrame::StfData{std::move(lHdrMsg), std::move(*pHbFramesBegin)});
    lStfNumMessages += 1;
    pHBFrameLen -= 1;
    pHbFramesBegin += 1;
  }

  while (pHBFrameLen > 0) {
    // check if we should cut at the next
    auto lDoneStfOpt = lReturnStfIfFinished(*pHbFramesBegin);
    if (lDoneStfOpt.has_value()) {
      return lDoneStfOpt;
    }

    lStf->addStfDataReadout(lDataId, pSubSpecification, SubTimeFrame::StfData{nullptr, std::move(*pHbFramesBegin)});
    lStfNumMessages += 1;
    pHBFrameLen -= 1;
    pHbFramesBegin += 1;
  }

  DDDLOG_RL(10000, "addTopoStfData: leaving without returning STF: numMessages={}", lStfNumMessages);

  return std::nullopt;
}


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileBuilder
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameFileBuilder::SubTimeFrameFileBuilder(SyncMemoryResources &pMemRes,
  const std::size_t pDataSegSize, const std::optional<std::uint16_t> pDataSegId,
  const std::size_t pHdrSegSize, const std::optional<std::uint16_t> pHdrSegId)
  : mMemRes(pMemRes)
{
  mMemRes.mDataMemRes = std::make_unique<DataRegionAllocatorResource>(
    "O2DataRegion_FileSource",
    pDataSegId, pDataSegSize,
    *mMemRes.mShmTransport,
    0 // Region flags
  );

  mMemRes.mHeaderMemRes = std::make_unique<RegionAllocatorResource< alignof(o2::header::DataHeader),
                                                                    RegionAllocStrategy::eFindFirst>> (
    "O2HeadersRegion_FileSource",
    pHdrSegId, pHdrSegSize,
    *mMemRes.mShmTransport,
    0, /* GPU flags */
    false /* cannot fail */
  );

  mMemRes.start();
}

void SubTimeFrameFileBuilder::adaptHeaders(SubTimeFrame *pStf)
{
  if (!pStf) {
    return;
  }

  for (auto& lDataIdentMapIter : pStf->mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {

        // make sure there is a DataProcessing header in the stack
        const auto &lHeader = lStfDataIter.mHeader;

        if (!lHeader || lHeader->GetSize() < sizeof(DataHeader)) {
          EDDLOG("File data invalid. Missing DataHeader.");
          return;
        }

        auto lDplHdrConst = o2::header::get<o2::framework::DataProcessingHeader*>(lHeader->GetData(), lHeader->GetSize());

        if (lDplHdrConst != nullptr) {
          auto lDplHdr = const_cast<o2::framework::DataProcessingHeader*>(lDplHdrConst);
          lDplHdr->startTime = pStf->header().mId;
          lDplHdr->creation = pStf->header().mCreationTimeMs;
        } else {
          // make the stack with an DPL header
          // get the DataHeader
          auto lDHdr = o2::header::get<o2::header::DataHeader*>(lHeader->GetData(), lHeader->GetSize());
          if (lDHdr == nullptr) {
            EDDLOG("File data invalid. DataHeader not found in the header stack.");
            return;
          }

          auto lDplHdr = o2::framework::DataProcessingHeader{pStf->header().mId};
          lDplHdr.creation = pStf->header().mCreationTimeMs;
          auto lStack = Stack(
            *lDHdr, /* TODO: add complete existing header lStfDataIter.mHeader */
            lDplHdr
          );

          lStfDataIter.mHeader = mMemRes.newHeaderMessage(lStack.data(), lStack.size());
          if (!lStfDataIter.mHeader) {
            return;
          }

        }
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// TimeFrameBuilder
////////////////////////////////////////////////////////////////////////////////

TimeFrameBuilder::TimeFrameBuilder(SyncMemoryResources &pMemRes)
  : mMemRes(pMemRes)
{
}

void TimeFrameBuilder::allocate_memory(const std::size_t pDataSegSize, const std::optional<std::uint16_t> pDataSegId,
                                       const std::size_t pHdrSegSize, const std::optional<std::uint16_t> pHdrSegId)
{
  mMemRes.mDataMemRes = std::make_unique<DataRegionAllocatorResource>(
    "O2DataRegion_TimeFrame",
    pDataSegId, pDataSegSize,
    *mMemRes.mShmTransport,
    0, // Region flags
    false /* cannot fail */
  );

  mMemRes.mHeaderMemRes = std::make_unique<HeaderRegionAllocatorResource>(
    "O2HeadersRegion",
    pHdrSegId, pHdrSegSize,
    *mMemRes.mShmTransport,
    0, /* GPU flags */
    false /* cannot fail */
  );

  mMemRes.start();
}

FairMQMessagePtr TimeFrameBuilder::newHeaderMessage(const char *pData, const std::size_t pSize)
{
    if (pSize < sizeof (DataHeader)) {
      EDDLOG_RL(1000, "TimeFrameBuilder: Header size less that DataHeader size={}", pSize);
      return nullptr;
    }

    // Get the DH
    DataHeader *lDataHdr = const_cast<DataHeader*>(reinterpret_cast<const DataHeader*>(pData));
    if (lDataHdr->description != DataHeader::sHeaderType) {
      EDDLOG_RL(1000, "TimeFrameBuilder: Unknown header type {}", lDataHdr->description.as<std::string>());
      return nullptr;
    }

    // quick check for missing DPL header
    if (pSize < (sizeof(DataHeader) + sizeof(o2::framework::DataProcessingHeader))) {
      EDDLOG("BUG: TimeFrameBuilder: missing DPL header");
    }

    return mMemRes.newHeaderMessage(pData, pSize);
}

void TimeFrameBuilder::adaptHeaders(SubTimeFrame *pStf)
{
  if (!pStf || !mMemRes.mHeaderMemRes || !mMemRes.mDataMemRes) {
    return;
  }

  const auto lOutChannelType = fair::mq::Transport::SHM;

  { // check if we have a valid creation time
    const auto lCreationTimeMs = pStf->header().mCreationTimeMs;
    if ((lCreationTimeMs == SubTimeFrame::Header::sInvalidTimeMs) || (lCreationTimeMs == 0)) {
      pStf->updateCreationTimeMs(); // use the current time
    }
  }

  const auto lCreationTimeMs = pStf->header().mCreationTimeMs;
  // adapt headers for DPL
  for (auto& lDataIdentMapIter : pStf->mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {

        // make sure there is a DataProcessing header in the stack
        const auto &lHeader = lStfDataIter.mHeader;

        if (!lHeader || lHeader->GetSize() < sizeof(DataHeader)) {
          EDDLOG("Adapting TF headers: Missing DataHeader.");
          continue;
        }

        auto lDataHdrConst = o2::header::get<o2::header::DataHeader*>(lHeader->GetData(), lHeader->GetSize());
        if (!lDataHdrConst) {
          EDDLOG("Adapting TF headers: Missing DataHeader get<DataHeader*>().");
          continue;
        }

        auto lDplHdrConst = o2::header::get<o2::framework::DataProcessingHeader*>(lHeader->GetData(), lHeader->GetSize());
        if (lDplHdrConst != nullptr) {
          auto lDplHdr = const_cast<o2::framework::DataProcessingHeader*>(lDplHdrConst);
          lDplHdr->startTime = pStf->header().mId;
          lDplHdr->creation = lCreationTimeMs;
        } else {
          // make the stack with an DPL header
          // get the DataHeader
          try {
            auto lDHdr = o2::header::get<o2::header::DataHeader*>(lHeader->GetData(), lHeader->GetSize());
            if (lDHdr == nullptr) {
              EDDLOG("TimeFrame invalid. DataHeader not found in the header stack.");
              continue;
            }
          } catch (std::exception& e) {
            EDDLOG("TimeFrame invalid: get<DataHeader>() failed. what={}", e.what());
            continue;
          }

          // add DPL header
          auto lDplHdr = o2::framework::DataProcessingHeader{pStf->header().mId};
          lDplHdr.creation = lCreationTimeMs;
          auto lStack = Stack(reinterpret_cast<std::byte*>(lHeader->GetData()), lDplHdr);

          WDDLOG_RL(5000, "Reallocation of Header messages is not optimal. orig_size={} new_size={}",
            lHeader->GetSize(), lStack.size());

          lStfDataIter.mHeader = newHeaderMessage(reinterpret_cast<char*>(lStack.data()), lStack.size());
          if (!lStfDataIter.mHeader) {
            return;
          }

        }
      }
    }
  }

  // make sure the data is in shmem messages if we are sending data to shmem channel
  if (lOutChannelType == fair::mq::Transport::SHM) {

    // adapt the payload for shmem if needed
    for (auto& lDataIdentMapIter : pStf->mData) {
      for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
        for (auto& lStfDataIter : lSubSpecMapIter.second) {

          // header
          {
            const auto &lHeaderMsg = lStfDataIter.mHeader;
            if (lHeaderMsg->GetType() != fair::mq::Transport::SHM) {
              WDDLOG_RL(1000, "adaptHeaders: Moving header message to SHM. size={}", lHeaderMsg->GetSize());
              auto lNewHdr = newHeaderMessage(reinterpret_cast<char*>(lHeaderMsg->GetData()), lHeaderMsg->GetSize());
              if (!lNewHdr) {
                return;
              }
              lStfDataIter.mHeader.swap(lNewHdr);
            }
          }

          // data
          for (auto &lDataMsg : lStfDataIter.mDataParts) {
            // const auto &lDataMsg = lStfDataIter.mData;
            if (lDataMsg->GetType() != fair::mq::Transport::SHM) {
              auto lNewDataMsg = newDataMessage(lDataMsg->GetSize());
              WDDLOG_RL(1000, "adaptHeaders: Moving data message to SHM. size={}", lDataMsg->GetSize());
              if (lNewDataMsg) {
                std::memcpy(lNewDataMsg->GetData(), lDataMsg->GetData(), lDataMsg->GetSize());
              } else {
                return;
              }
              lDataMsg.swap(lNewDataMsg);
            }
          }
        }
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameCopyBuilder
////////////////////////////////////////////////////////////////////////////////

void SubTimeFrameCopyBuilder::allocate_memory(const std::size_t pDataSegSize, const std::optional<std::uint16_t> pDataSegId)
{
  mMemRes.mDataMemRes = std::make_unique<DataRegionAllocatorResource>(
    "O2DataRegion_TimeFrame",
    pDataSegId, pDataSegSize,
    *mMemRes.mShmTransport,
    0, // Region flags
    true /* can fail */
  );

  mMemRes.mHeaderMemRes = nullptr; // we don't copy o2HDRs

  mMemRes.start();
}

bool SubTimeFrameCopyBuilder::copyStfData(std::unique_ptr<SubTimeFrame> &pStf)
{
  for (auto& lDataIdentMapIter : pStf->mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {
        // replace and copy data FMQ messages
        if (!mMemRes.replaceDataMessages(lStfDataIter.mDataParts)) {
          return false;
        }
      }
    }
  }

  return true;
}

} /* o2::DataDistribution */
