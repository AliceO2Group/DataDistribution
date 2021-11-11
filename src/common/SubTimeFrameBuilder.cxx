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

SubTimeFrameReadoutBuilder::SubTimeFrameReadoutBuilder(MemoryResources &pMemRes, bool pDplEnabled)
  : mStf(nullptr), mDplEnabled(pDplEnabled),mMemRes(pMemRes)
{
  mMemRes.mHeaderMemRes = std::make_unique<RegionAllocatorResource<alignof(o2::header::DataHeader)>>(
    "O2HeadersRegion",
    *mMemRes.mShmTransport,
    std::size_t(512) << 20, /* good for 5s 3CRU @ 50Gbps, TODO: make configurable */
    RegionAllocStrategy::eFindFirst,
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

  if (pHdr.mTimeframeOrbitFirst != 0) {
    mStf->updateFirstOrbit(pHdr.mTimeframeOrbitFirst);
  } else {
    WDDLOG_RL(1000, "READOUT INTERFACE: First orbit in TF is not set.");
    try {
      const auto R = RDHReader(pHbFramesBegin[0]);
      mStf->updateFirstOrbit(R.getOrbit());
    } catch (...) {
      EDDLOG("Error getting RDHReader instace. Not using {} HBFs", pHBFrameLen);
      return false;
    }
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

  for (size_t i = 0; i < pHBFrameLen; i++) {

    if (lRemoveBlocks[i]) {
      continue; // already filtered out
    }

    std::unique_ptr<FairMQMessage> lHdrMsg;

    lDataHdr.payloadSize = pHbFramesBegin[i]->GetSize();

    if (mDplEnabled) {
      auto lDplHdr = o2::framework::DataProcessingHeader{mStf->header().mId};
      lDplHdr.creation = mStf->header().mCreationTimeMs;
      auto lStack = Stack(
        lDataHdr,
        lDplHdr
      );

      lHdrMsg = mMemRes.newHeaderMessage(reinterpret_cast<char*>(lStack.data()), lStack.size());
    } else {
      lHdrMsg = mMemRes.newHeaderMessage(reinterpret_cast<char*>(&lDataHdr), sizeof(lDataHdr));
    }

    if (!lHdrMsg) {
      WDDLOG_RL(1000, "Allocation error: dropping data of the current STF stf_id={}", pHdr.mRunNumber);

      // clear data of the partial STF
      mAcceptStfData = false;
      mStf->clear();

      return false;
    }

    mStf->addStfData(lDataHdr,
      SubTimeFrame::StfData{ std::move(lHdrMsg), std::move(pHbFramesBegin[i]) }
    );
  }
  return true;
}

std::optional<std::unique_ptr<SubTimeFrame>> SubTimeFrameReadoutBuilder::addTopoStfData(
  const o2::header::DataOrigin &pDataOrig,
  const o2::header::DataHeader::SubSpecificationType pSubSpecification,
  const ReadoutSubTimeframeHeader& pHdr,
  std::vector<FairMQMessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen,
  const std::uint64_t pMaxNumMessages)
{
  static uint32_t sTfId = 1;

  if (!mRunning) {
    WDDLOG("Adding HBFrames while STFBuilder is not running!");
    return std::nullopt;
  }

  if (!mAcceptStfData) {
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

  DataHeader lDataHdr(
    o2::header::gDataDescriptionRawData,
    pDataOrig,
    pSubSpecification,
    0 /* Update later */
  );
  lDataHdr.payloadSerializationMethod = gSerializationMethodNone;

  for (size_t i = 0; i < pHBFrameLen; i++) {
    std::unique_ptr<FairMQMessage> lHdrMsg;

    lDataHdr.payloadSize = pHbFramesBegin[i]->GetSize();

    if (mDplEnabled) {
      auto lDplHdr = o2::framework::DataProcessingHeader{lStf->header().mId};
      lDplHdr.creation = lStf->header().mCreationTimeMs;
      auto lStack = Stack(
        lDataHdr,
        lDplHdr
      );

      lHdrMsg = mMemRes.newHeaderMessage(reinterpret_cast<char*>(lStack.data()), lStack.size());
    } else {
      lHdrMsg = mMemRes.newHeaderMessage(reinterpret_cast<char*>(&lDataHdr), sizeof(lDataHdr));
    }

    if (!lHdrMsg) {
      WDDLOG_RL(1000, "Allocation error: dropping data of the current STF stf_id={}", pHdr.mRunNumber);
      // clear data of the partial STF
      mAcceptStfData = false;
      lStf->clear();

      return std::nullopt;
    }

    lStf->addStfData(lDataHdr, SubTimeFrame::StfData{ std::move(lHdrMsg), std::move(pHbFramesBegin[i]) } );
    lStfNumMessages += 1;
  }

  if (lStfNumMessages >= pMaxNumMessages) {
    lStf->setOrigin(SubTimeFrame::Header::Origin::eReadoutTopology);
    std::optional<std::unique_ptr<SubTimeFrame>> lRetStf = std::move(lStf);

    IDDLOG_RL(1000, "addTopoStfData: leaving and returning STF: numMessages={}", lStfNumMessages);
    lStfNumMessages = 0;
    return lRetStf;
  }

  IDDLOG_RL(1000, "addTopoStfData: leaving without returning STF: numMessages={}", lStfNumMessages);

  return std::nullopt;
}


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileBuilder
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameFileBuilder::SubTimeFrameFileBuilder(MemoryResources &pMemRes,
  const std::size_t pDataSegSize, const std::size_t pHdrSegSize, bool pDplEnabled)
  : mMemRes(pMemRes), mDplEnabled(pDplEnabled)
{
  mMemRes.mHeaderMemRes = std::make_unique<RegionAllocatorResource<alignof(o2::header::DataHeader)>>(
    "O2HeadersRegion_FileSource",
    *mMemRes.mShmTransport,
    pHdrSegSize,
    RegionAllocStrategy::eFindFirst,
    0, /* GPU flags */
    false /* cannot fail */
  );

  mMemRes.mDataMemRes = std::make_unique<RegionAllocatorResource<>>(
    "O2DataRegion_FileSource",
    *mMemRes.mShmTransport,
    pDataSegSize,
    RegionAllocStrategy::eFindLongest,
    0 // TODO: GPU flags
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

          if (mDplEnabled) {
            auto lDplHdr = o2::framework::DataProcessingHeader{pStf->header().mId};
            lDplHdr.creation = pStf->header().mCreationTimeMs;
            auto lStack = Stack(
              *lDHdr, /* TODO: add complete existing header lStfDataIter.mHeader */
              lDplHdr
            );

            lStfDataIter.mHeader = mMemRes.newHeaderMessage(reinterpret_cast<char*>(lStack.data()), lStack.size());
            if (!lStfDataIter.mHeader) {
              return;
            }
          }
        }
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// TimeFrameBuilder
////////////////////////////////////////////////////////////////////////////////

TimeFrameBuilder::TimeFrameBuilder(SyncMemoryResources &pMemRes, bool pDplEnabled)
  : mDplEnabled(pDplEnabled), mMemRes(pMemRes)
{
}

void TimeFrameBuilder::allocate_memory(const std::size_t pDataSegSize, const std::size_t pHdrSegSize)
{
  mMemRes.mHeaderMemRes = std::make_unique<RegionAllocatorResource<alignof(o2::header::DataHeader)>>(
    "O2HeadersRegion",
    *mMemRes.mShmTransport,
    pHdrSegSize,
    RegionAllocStrategy::eFindFirst,
    0, /* GPU flags */
    false /* cannot fail */
  );

  mMemRes.mDataMemRes = std::make_unique<RegionAllocatorResource<>>(
    "O2DataRegion_TimeFrame",
    *mMemRes.mShmTransport,
    pDataSegSize,
    RegionAllocStrategy::eFindLongest,
    0, /* TODO: GPU flags */
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
      EDDLOG_RL(1000, "TimeFrameBuilder: Unknown header type {}", std::string(lDataHdr->description.str));
      return nullptr;
    }

    // clear last bit set in data header
    if (pSize == sizeof(DataHeader)) {
      lDataHdr->flagsNextHeader = 0; // DataHeader is alone
    }

    // check if already have DataProcessing header
    const o2::framework::DataProcessingHeader* lDplHdr = nullptr;
    try {
      lDplHdr = o2::header::get<o2::framework::DataProcessingHeader*>(pData, pSize);
    } catch(...) {
      lDplHdr = nullptr;
    }

    if (mDplEnabled && !lDplHdr) {
      auto lStack = Stack(
        *lDataHdr,
        o2::framework::DataProcessingHeader{lDataHdr->tfCounter}
      );
      return mMemRes.newHeaderMessage(reinterpret_cast<char*>(lStack.data()), lStack.size());
    }

    if (!mDplEnabled && lDplHdr) {
      // remove processing header
      const auto lNewSize = reinterpret_cast<const char*>(lDplHdr) - pData;
      lDataHdr->flagsNextHeader = 0; // DataHeader is alone
      return mMemRes.newHeaderMessage(pData, lNewSize);
    }

    return mMemRes.newHeaderMessage(pData, pSize);
}

void TimeFrameBuilder::adaptHeaders(SubTimeFrame *pStf)
{
  if (!pStf || !mMemRes.mHeaderMemRes || !mMemRes.mDataMemRes) {
    return;
  }

  const auto lOutChannelType = fair::mq::Transport::SHM;

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

        auto lDataHdrConst = o2::header::get<o2::header::DataHeader*>(
          lHeader->GetData(),
          lHeader->GetSize()
        );

        if (!lDataHdrConst) {
          EDDLOG("Adapting TF headers: Missing DataHeader get<DataHeader*>().");
          continue;
        }

        auto lDplHdrConst = o2::header::get<o2::framework::DataProcessingHeader*>(
          lHeader->GetData(),
          lHeader->GetSize()
        );

        if (lDplHdrConst != nullptr) {
          auto lDplHdr = const_cast<o2::framework::DataProcessingHeader*>(lDplHdrConst);
          lDplHdr->startTime = pStf->header().mId;
          lDplHdr->creation = pStf->header().mCreationTimeMs;
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

          if (mDplEnabled) {
            auto lDplHdr = o2::framework::DataProcessingHeader{pStf->header().mId};
            lDplHdr.creation = pStf->header().mCreationTimeMs;
            auto lStack = Stack(
              reinterpret_cast<std::byte*>(lHeader->GetData()),
              lDplHdr
            );

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
          {
            const auto &lDataMsg = lStfDataIter.mData;
            if (lDataMsg->GetType() != fair::mq::Transport::SHM) {
              auto lNewDataMsg = newDataMessage(lDataMsg->GetSize());
              WDDLOG_RL(1000, "adaptHeaders: Moving data message to SHM. size={}", lDataMsg->GetSize());
              if (lNewDataMsg) {
                std::memcpy(lNewDataMsg->GetData(), lDataMsg->GetData(), lDataMsg->GetSize());
              } else {
                return;
              }
              lStfDataIter.mData.swap(lNewDataMsg);
            }
          }
        }
      }
    }
  }
}

} /* o2::DataDistribution */
