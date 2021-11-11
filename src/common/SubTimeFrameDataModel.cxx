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

#include "ReadoutDataModel.h"
#include "SubTimeFrameDataModel.h"

#include "DataDistLogger.h"

#include <map>
#include <iterator>
#include <algorithm>

namespace o2::DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrame
////////////////////////////////////////////////////////////////////////////////
SubTimeFrame::SubTimeFrame(uint64_t pStfId)
  : mHeader(pStfId)
{
  updateCreationTimeMs(); // set the current creation time
}

void SubTimeFrame::updateStf() const
{
  if (mDataUpdated) {
    return;
  }

  // recalculate data size
  mDataSize = 0;

  // Update data block indexes
  for (auto &lIdentSubSpecVect : mData) {
    StfSubSpecMap &lSubSpecMap = lIdentSubSpecVect.second;

    for (auto &lSubSpecDataVector : lSubSpecMap) {
      StfDataVector &lDataVector = lSubSpecDataVector.second;

      const auto lTotalCount = lDataVector.size();
      for (StfDataVector::size_type i = 0; i < lTotalCount; i++) {

        lDataVector[i].setPayloadIndex_TfCounter_RunNumber(i, lTotalCount, mHeader.mId, mHeader.mRunNumber);

        // update first orbit if not present in the data (old tf files)
        // update tfCounter
        if (mHeader.mFirstOrbit != std::numeric_limits<std::uint32_t>::max()) {
          lDataVector[i].setFirstOrbit(mHeader.mFirstOrbit);
        }

        // sum up only data size
        mDataSize += lDataVector[i].mData->GetSize();
      }

      assert(lDataVector.empty() ? true :
        lDataVector.front().getDataHeader().splitPayloadIndex == 0
      );
      assert(lDataVector.empty() ? true :
        lDataVector.back().getDataHeader().splitPayloadIndex == (lTotalCount - 1)
      );
      assert(lDataVector.empty() ? true :
        lDataVector.front().getDataHeader().splitPayloadParts == lTotalCount
      );
      assert(lDataVector.empty() ? true :
        lDataVector.front().getDataHeader().splitPayloadParts ==
        lDataVector.back().getDataHeader().splitPayloadParts
      );
    }
  }
  mDataUpdated = true;
}

std::vector<EquipmentIdentifier> SubTimeFrame::getEquipmentIdentifiers() const
{
  std::vector<EquipmentIdentifier> lKeys;

  for (const auto& lDataIdentMapIter : mData) {
    for (const auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      lKeys.emplace_back(lDataIdentMapIter.first, lSubSpecMapIter.first);
    }
  }

  return lKeys;
}

void SubTimeFrame::mergeStf(std::unique_ptr<SubTimeFrame> pStf, const std::string &mStfSenderId)
{
  // incoming empty STF
  if ((pStf->header().mOrigin == Header::Origin::eNull) && (pStf->getDataSize() == 0)) {
    return; // nothing to do for an empty STF
  }

  // are we starting with an empty STF? ... use the next valied one for the header
  if (mHeader.mOrigin == Header::Origin::eNull) {
    mHeader = pStf->header();
  }

  // make sure header values match
  if (mHeader.mOrigin != pStf->header().mOrigin) {
    EDDLOG_RL(5000, "Merging STFs error: STF origins do not match origin={} new_origin={} new_stfs_id={}",
      mHeader.mOrigin,  pStf->header().mOrigin, mStfSenderId);
  }

  if (mHeader.mFirstOrbit != pStf->header().mFirstOrbit) {
    EDDLOG_RL(5000,"Merging STFs error: STF first orbits do not match firstOrbit={} new_firstOrbit={} diff={} new_stfs_id={}",
      mHeader.mFirstOrbit,  pStf->header().mFirstOrbit, (std::int64_t(pStf->header().mFirstOrbit) - std::int64_t(mHeader.mFirstOrbit)),
      mStfSenderId);
  }

  // make sure data equipment does not repeat
  std::set<EquipmentIdentifier> lUnionSet;
  for (const auto& lId : getEquipmentIdentifiers()) {
    lUnionSet.emplace(lId);
  }

  for (const auto& lId : pStf->getEquipmentIdentifiers()) {
    if (lUnionSet.emplace(lId).second == false /* not inserted */) {
      IDDLOG_RL(5000, "Merging STFs error: Equipment already present: fee={} new_stfs_id={}", lId.info(), mStfSenderId);
    }
  }

  // merge the Stfs
  for (auto& lDataIdentMapIter : pStf->mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      // source
      const DataIdentifier& lDataId = lDataIdentMapIter.first;
      const DataHeader::SubSpecificationType& lSubSpec = lSubSpecMapIter.first;
      StfDataVector& lStfDataVec = lSubSpecMapIter.second;

      // destination
      StfDataVector& lDstStfDataVec = mData[lDataId][lSubSpec];

      std::move(
        lStfDataVec.begin(),
        lStfDataVec.end(),
        std::back_inserter(lDstStfDataVec));
    }
  }

  // delete pStf
  pStf.reset();
}

void SubTimeFrame::removeRedundantHeaders()
{
  // Update data block indexes
  for (auto &lIdentSubSpecVect : mData) {
    StfSubSpecMap &lSubSpecMap = lIdentSubSpecVect.second;

    for (auto &lSubSpecDataVector : lSubSpecMap) {
      StfDataVector &lDataVector = lSubSpecDataVector.second;

      // Only remove if RAWDATA
      if (!lDataVector.empty() && lDataVector[0].mHeader) {
        o2hdr::DataHeader *lDataHdr = reinterpret_cast<o2hdr::DataHeader*>(lDataVector[0].mHeader->GetData());
        if (lDataHdr->dataDescription != gDataDescriptionRawData) {
          continue;
        }
      }

      // leave the first header
      for (StfDataVector::size_type i = 1; i < lDataVector.size(); i++) {

        if(lDataVector[i].mHeader) {

          lDataVector[i].mHeader.reset(nullptr);
        }
      }
    }
  }
}

} /* o2::DataDistribution */
