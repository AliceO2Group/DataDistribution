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

std::uint64_t SubTimeFrame::getDataSize() const
{
  std::uint64_t lDataSize = std::uint64_t(0);

  for (const auto& lDataIdentMapIter : mData) {
    for (const auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (const auto& lStfDataIter : lSubSpecMapIter.second) {
        lDataSize += lStfDataIter.mData->GetSize();
      }
    }
  }

  return lDataSize;
}

std::vector<EquipmentIdentifier> SubTimeFrame::getEquipmentIdentifiers() const
{
  std::vector<EquipmentIdentifier> lKeys;

  for (const auto& lDataIdentMapIter : mData) {
    for (const auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      lKeys.emplace_back(EquipmentIdentifier(lDataIdentMapIter.first, lSubSpecMapIter.first));
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
      DDLOGF(fair::Severity::ERROR, "Mergingin STFs error: Equipment already present: fee={}", lId.info());
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
