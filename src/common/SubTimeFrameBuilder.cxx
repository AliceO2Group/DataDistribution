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

SubTimeFrameReadoutBuilder::SubTimeFrameReadoutBuilder(FairMQChannel& pChan, bool pDplEnabled)
  : mStf(nullptr),
    mDplEnabled(pDplEnabled)
{
  mHeaderMemRes = std::make_unique<FMQUnsynchronizedPoolMemoryResource>(
    pChan, 64ULL << 20 /* make configurable */,
    mDplEnabled ?
      sizeof(DataHeader) + sizeof(o2::framework::DataProcessingHeader) :
      sizeof(DataHeader)
  );
}

void SubTimeFrameReadoutBuilder::addHbFrames(
  const o2::header::DataOrigin &pDataOrig,
  const o2::header::DataHeader::SubSpecificationType pSubSpecification,
  ReadoutSubTimeframeHeader& pHdr,
  std::vector<FairMQMessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen)
{
  if (!mStf) {
    mStf = std::make_unique<SubTimeFrame>(pHdr.mTimeFrameId);
  }

  std::vector<bool> lKeepBlocks(pHBFrameLen, true);

  // filter empty trigger RDHv4
  {
    if (mRdh4FilterTrigger) {
      // filter 2 empty 8kiB pages
      if (pHBFrameLen == 2 && lKeepBlocks[0] == true && lKeepBlocks[1] == true) {
        if (pHbFramesBegin[0]->GetSize() == 8192 && pHbFramesBegin[1]->GetSize() == 8192) {

          bool lRem1 = false, lRem2 = false;
          {
            const auto [lMemSize, lOffsetNext, lStopBit] = ReadoutDataUtils::getRdhNavigationVals(
              reinterpret_cast<const char*>(pHbFramesBegin[0]->GetData()));

            (void) lOffsetNext; /*unused*/

            if (lStopBit && lMemSize == 64) {
              lRem1 = true;
            }
          }

          {
            const auto [lMemSize, lOffsetNext, lStopBit] = ReadoutDataUtils::getRdhNavigationVals(
              reinterpret_cast<const char*>(pHbFramesBegin[1]->GetData()));

            (void) lOffsetNext; /*unused*/

            if (lStopBit && lMemSize == 64) {
              lRem2 = true;
            }
          }
          if (lRem1 && lRem2) {
            lKeepBlocks[0] = false;
            lKeepBlocks[1] = false;
          }
        }
      }

      // filter empty 16kiB start stop pages
      for (std::size_t i = 0; i < pHBFrameLen; i++) {
        if (lKeepBlocks[i] == false) {
          continue; // already discarded
        }

        if (!ReadoutDataUtils::filterTriggerEmpyBlocksV4(
                                  reinterpret_cast<const char*>(pHbFramesBegin[i]->GetData()),
                                  pHbFramesBegin[i]->GetSize()) ) {
          lKeepBlocks[i] = true;
        } else {
          lKeepBlocks[i] = false;
        }
      }
    }
  }


  // sanity check
  {
    if (RdhSanityCheck() != ReadoutDataUtils::eNoSanityCheck) {

      // check blocks individually
      for (std::size_t i = 0; i < pHBFrameLen; i++) {

        if (lKeepBlocks[i] == false) {
          continue; // already filtered out
        }

        const auto lOk = ReadoutDataUtils::rdhSanityCheck(
          reinterpret_cast<const char*>(pHbFramesBegin[i]->GetData()),
          pHbFramesBegin[i]->GetSize());

        if (!lOk && RdhSanityCheck() == ReadoutDataUtils::eSanityCheckDrop) {
          DDLOG(fair::Severity::WARNING) << "RDH SANITY CHECK: Removing data block";

          lKeepBlocks[i] = false;

        } else if (!lOk && RdhSanityCheck() == ReadoutDataUtils::eSanityCheckPrint) {

          DDLOG(fair::Severity::INFO) << "Printing data blocks of update with TF ID: " << pHdr.mTimeFrameId
                    << ", Link ID: " << unsigned(pHdr.mLinkId);

          // dump the data block, skipping data
          std::size_t lCurrentDataIdx = 0;

          while (lCurrentDataIdx < pHbFramesBegin[i]->GetSize()) {

            const auto lDataSizeLeft = std::size_t(pHbFramesBegin[i]->GetSize()) - lCurrentDataIdx;

            std::string lInfoStr = "RDH block (64 bytes in total) of [";
            lInfoStr += std::to_string(i) + "] 8 kiB page";

            o2::header::hexDump(lInfoStr.c_str(),
              reinterpret_cast<char*>(pHbFramesBegin[i]->GetData()) + lCurrentDataIdx,
              std::size_t(std::min(std::size_t(64), lDataSizeLeft)));

            auto [lCru, lEp, lLink] = ReadoutDataUtils::getSubSpecificationComponents(
              reinterpret_cast<char*>(pHbFramesBegin[i]->GetData()) + lCurrentDataIdx,
              std::size_t(std::min(std::size_t(64), lDataSizeLeft))
            );
            DDLOG(fair::Severity::INFO) << "RDH info CRU: " << lCru << " Endpoint: " << lEp << " Link: " << lLink;

            const auto [lMemSize, lOffsetNext, lStopBit] = ReadoutDataUtils::getRdhNavigationVals(
              reinterpret_cast<const char*>(pHbFramesBegin[i]->GetData()) + lCurrentDataIdx);

            (void) lMemSize; /*unused*/
            (void) lStopBit; /*unused*/

            lCurrentDataIdx += std::min(std::size_t(lOffsetNext), lDataSizeLeft);
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

  for (size_t i = 0; i < pHBFrameLen; i++) {

    if (lKeepBlocks[i] == false) {
      continue; // already filtered out
    }

    std::unique_ptr<FairMQMessage> lHdrMsg;

    DataHeader lDataHdr(
      lEqId.mDataDescription,
      lEqId.mDataOrigin,
      lEqId.mSubSpecification,
      pHbFramesBegin[i]->GetSize()
    );
    lDataHdr.payloadSerializationMethod = gSerializationMethodNone;

    if (mDplEnabled) {
      auto lStack = Stack(mHeaderMemRes->allocator(),
        lDataHdr,
        o2::framework::DataProcessingHeader{mStf->header().mId}
      );

      lHdrMsg = mHeaderMemRes->NewFairMQMessageFromPtr(lStack.data());
    } else {
      auto lHdrMsgStack = Stack(mHeaderMemRes->allocator(), lDataHdr);
      lHdrMsg = mHeaderMemRes->NewFairMQMessageFromPtr(lHdrMsgStack.data());
    }

    if (!lHdrMsg) {
      DDLOG(fair::Severity::ERROR) << "Allocation error: HbFrame::DataHeader: " << sizeof(DataHeader);
      throw std::bad_alloc();
    }

    mStf->addStfData(lDataHdr,
      SubTimeFrame::StfData{ std::move(lHdrMsg), std::move(pHbFramesBegin[i]) }
    );
  }

}

std::unique_ptr<SubTimeFrame> SubTimeFrameReadoutBuilder::getStf()
{
  std::unique_ptr<SubTimeFrame> lStf = std::move(mStf);
  mStf = nullptr;
  return lStf;
}


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileBuilder
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameFileBuilder::SubTimeFrameFileBuilder(FairMQChannel& pChan, bool pDplEnabled)
  : mDplEnabled(pDplEnabled)
{
  mHeaderMemRes = std::make_unique<FMQUnsynchronizedPoolMemoryResource>(
    pChan, 32ULL << 20 /* make configurable */,
    mDplEnabled ?
      sizeof(DataHeader) + sizeof(o2::framework::DataProcessingHeader) :
      sizeof(DataHeader)
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
          DDLOG(fair::Severity::ERROR) << "File data invalid. Missing DataHeader.";
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
            DDLOG(fair::Severity::ERROR) << "File data invalid. DataHeader not found in the header stack.";
            return;
          }

          if (mDplEnabled) {
            auto lStack = Stack(mHeaderMemRes->allocator(),
              *lDHdr, /* TODO: add complete existing header lStfDataIter.mHeader */
              o2::framework::DataProcessingHeader{pStf->header().mId}
            );

            lStfDataIter.mHeader = mHeaderMemRes->NewFairMQMessageFromPtr(lStack.data());
            assert(lStfDataIter.mHeader->GetSize() > sizeof (DataHeader));
          }
        }
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// TimeFrameBuilder
////////////////////////////////////////////////////////////////////////////////

TimeFrameBuilder::TimeFrameBuilder(FairMQChannel& pChan, bool pDplEnabled)
  : mDplEnabled(pDplEnabled)
{
  mHeaderMemRes = std::make_unique<FMQUnsynchronizedPoolMemoryResource>(
    pChan, 64ULL << 20 /* make configurable */,
    mDplEnabled ?
      sizeof(DataHeader) + sizeof(o2::framework::DataProcessingHeader) :
      sizeof(DataHeader)
  );
}

void TimeFrameBuilder::adaptHeaders(SubTimeFrame *pStf)
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
          DDLOG(fair::Severity::ERROR) << "Adapting TF headers: Missing DataHeader.";
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
            DDLOG(fair::Severity::ERROR) << "TimeFrame invalid. DataHeader not found in the header stack.";
            continue;
          }

          if (mDplEnabled) {
            auto lStack = Stack(mHeaderMemRes->allocator(),
              *lDHdr, /* TODO: add complete existing header lStfDataIter.mHeader */
              o2::framework::DataProcessingHeader{pStf->header().mId}
            );

            lStfDataIter.mHeader = mHeaderMemRes->NewFairMQMessageFromPtr(lStack.data());
            assert(lStfDataIter.mHeader->GetSize() > sizeof (DataHeader));
          }
        }
      }
    }
  }
}


}
} /* o2::DataDistribution */
