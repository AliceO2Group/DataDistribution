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

namespace o2
{
namespace DataDistribution
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
    std::size_t(256) << 20, /* make configurable */
    mDplEnabled ?
      sizeof(DataHeader) + sizeof(o2::framework::DataProcessingHeader) :
      sizeof(DataHeader)
  );
}

void SubTimeFrameReadoutBuilder::addHbFrames(
  const o2::header::DataOrigin &pDataOrig,
  const o2::header::DataHeader::SubSpecificationType pSubSpecification,
  const ReadoutSubTimeframeHeader& pHdr,
  std::vector<FairMQMessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen)
{
  static thread_local std::vector<bool> lRemoveBlocks;

  if (!mRunning) {
    WDDLOG("Adding HBFrames while STFBuilder is not running!");
    return;
  }

  if (!mStf) {
    mStf = std::make_unique<SubTimeFrame>(pHdr.mTimeFrameId);
    mFirstFiltered.clear();
  }

  // TODO: remove when the readout supplies the value
  try {
    const auto R = RDHReader(pHbFramesBegin[0]);
    mStf->updateFirstOrbit(R.getOrbit());
  } catch (...) {
    EDDLOG("Error getting RDHReader instace. Not using {} HBFs", pHBFrameLen);
    return;
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

  const EquipmentIdentifier lEqId = EquipmentIdentifier(
    o2::header::gDataDescriptionRawData,
    pDataOrig,
    pSubSpecification);

  DataHeader lDataHdr(
    lEqId.mDataDescription,
    lEqId.mDataOrigin,
    lEqId.mSubSpecification,
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
      auto lStack = Stack(
        lDataHdr,
        o2::framework::DataProcessingHeader{mStf->header().mId}
      );

      lHdrMsg = mMemRes.newHeaderMessage(lStack.size());
      if (lHdrMsg) {
        std::memcpy(lHdrMsg->GetData(), lStack.data(), lStack.size());
      }
    } else {
      auto lHdrMsgStack = Stack(lDataHdr);

      lHdrMsg = mMemRes.newHeaderMessage(lHdrMsgStack.size());
      if (lHdrMsg) {
        std::memcpy(lHdrMsg->GetData(), lHdrMsgStack.data(), lHdrMsgStack.size());
      }
    }

    if (!lHdrMsg) {
      EDDLOG("Allocation error: HbFrame::DataHeader={}", sizeof(DataHeader));
      throw std::bad_alloc();
    }

    mStf->addStfData(lDataHdr,
      SubTimeFrame::StfData{ std::move(lHdrMsg), std::move(pHbFramesBegin[i]) }
    );
  }

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
    0
  );

  mMemRes.mDataMemRes = std::make_unique<RegionAllocatorResource<>>(
    "O2DataRegion_FileSource",
    *mMemRes.mShmTransport,
    pDataSegSize,
    0 // TODO: GPU flags
  );
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
          if (lDplHdrConst->startTime != pStf->header().mId) {

            auto lDplHdr = const_cast<o2::framework::DataProcessingHeader*>(lDplHdrConst);
            lDplHdr->startTime = pStf->header().mId;
          }
        } else {
          // make the stack with an DPL header
          // get the DataHeader
          auto lDHdr = o2::header::get<o2::header::DataHeader*>(lHeader->GetData(), lHeader->GetSize());
          if (lDHdr == nullptr) {
            EDDLOG("File data invalid. DataHeader not found in the header stack.");
            return;
          }

          if (mDplEnabled) {
            auto lStack = Stack(
              *lDHdr, /* TODO: add complete existing header lStfDataIter.mHeader */
              o2::framework::DataProcessingHeader{pStf->header().mId}
            );

            lStfDataIter.mHeader = mMemRes.newHeaderMessage(lStack.size());
            if (lStfDataIter.mHeader) {
              assert(lStfDataIter.mHeader->GetSize() > sizeof (DataHeader));
              std::memcpy(lStfDataIter.mHeader->GetData(), lStack.data(), lStack.size());
            } else {
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

TimeFrameBuilder::TimeFrameBuilder(MemoryResources &pMemRes,
  const std::size_t pDataSegSize, const std::size_t pHdrSegSize, bool pDplEnabled)
  : mDplEnabled(pDplEnabled), mMemRes(pMemRes)
{
  mMemRes.mHeaderMemRes = std::make_unique<RegionAllocatorResource<alignof(o2::header::DataHeader)>>(
    "O2HeadersRegion",
    *mMemRes.mShmTransport,
    pHdrSegSize,
    0 /* dont need registration flags for headers */
  );

  mMemRes.mDataMemRes = std::make_unique<RegionAllocatorResource<>>(
    "O2DataRegion_FileSource",
    *mMemRes.mShmTransport,
    pDataSegSize,
    0 // TODO: GPU flags
  );
}

void TimeFrameBuilder::adaptHeaders(SubTimeFrame *pStf)
{
  if (!pStf) {
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

        auto lDplHdrConst = o2::header::get<o2::framework::DataProcessingHeader*>(
          lHeader->GetData(),
          lHeader->GetSize()
        );

        if (lDplHdrConst != nullptr) {
          if (lDplHdrConst->startTime != pStf->header().mId) {

            auto lDplHdr = const_cast<o2::framework::DataProcessingHeader*>(lDplHdrConst);
            lDplHdr->startTime = pStf->header().mId;
          }
        } else {
          // make the stack with an DPL header
          // get the DataHeader
          auto lDHdr = o2::header::get<o2::header::DataHeader*>(
            lHeader->GetData(),
            lHeader->GetSize()
          );

          if (lDHdr == nullptr) {
            EDDLOG("TimeFrame invalid. DataHeader not found in the header stack.");
            continue;
          }

          if (mDplEnabled) {
            auto lStack = Stack(
              reinterpret_cast<o2::byte*>(lHeader->GetData()),
              o2::framework::DataProcessingHeader{pStf->header().mId}
            );

            lStfDataIter.mHeader = newHeaderMessage(lStack.size());

            if (lStfDataIter.mHeader) {
              assert(lStfDataIter.mHeader->GetSize() >= sizeof (DataHeader));
              std::memcpy(lStfDataIter.mHeader->GetData(), lStack.data(), lStack.size());
            } else {
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
              auto lNewHdr = newHeaderMessage(lHeaderMsg->GetSize());
              if (lNewHdr) {
                std::memcpy(lNewHdr->GetData(), lHeaderMsg->GetData(), lHeaderMsg->GetSize());
              } else {
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

}
} /* o2::DataDistribution */
