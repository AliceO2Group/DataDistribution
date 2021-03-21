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

namespace o2
{
namespace DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrame
////////////////////////////////////////////////////////////////////////////////
SubTimeFrame::SubTimeFrame(uint64_t pStfId)
  : mHeader(pStfId)
{
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
        lDataVector[i].setPayloadIndex(i, lTotalCount);

        // update first orbit if not present in the data (old tf files)
        // update tfCounter: TODO: incrementing always for looping of the same data
        // update runNumber: TODO: zero for now.
        if (mHeader.mFirstOrbit != std::numeric_limits<std::uint32_t>::max()) {
          lDataVector[i].setFirstOrbit(mHeader.mFirstOrbit);
        }

        lDataVector[i].setTfCounter(mHeader.mId);
        lDataVector[i].setRunNumber(mHeader.mRunNumber);

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

void SubTimeFrame::mergeStf(std::unique_ptr<SubTimeFrame> pStf)
{
  // make sure data equipment does not repeat
  std::set<EquipmentIdentifier> lUnionSet;
  for (const auto& lId : getEquipmentIdentifiers())
    lUnionSet.emplace(lId);

  for (const auto& lId : pStf->getEquipmentIdentifiers()) {
    if (lUnionSet.emplace(lId).second == false /* not inserted */) {
      EDDLOG("Merging STFs error: Equipment already present: fee={}", lId.info());
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
}
} /* o2::DataDistribution */
