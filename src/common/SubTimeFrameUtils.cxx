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

#include "SubTimeFrameUtils.h"
#include "SubTimeFrameVisitors.h"
#include "SubTimeFrameDataModel.h"
#include "DataModelUtils.h"

#include <stdexcept>

#include <vector>
#include <deque>

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// DataOriginSplitter
////////////////////////////////////////////////////////////////////////////////

void DataIdentifierSplitter::visit(SubTimeFrame& pStf)
{
  std::vector<DataIdentifier> lToErase;

  if (mDataIdentifier.dataOrigin == gDataOriginAny) {
    mSubTimeFrame = std::make_unique<SubTimeFrame>(std::move(pStf));
  } else if (mDataIdentifier.dataDescription == gDataDescriptionAny) {
    // filter any source with requested origin
    mSubTimeFrame = std::make_unique<SubTimeFrame>(pStf.header().mId);
    for (auto& lKeyData : pStf.mData) {
      const DataIdentifier& lIden = lKeyData.first;
      if (lIden.dataOrigin == mDataIdentifier.dataOrigin) {
        // use the equipment identifier of the object
        mSubTimeFrame->mData[lIden] = std::move(lKeyData.second);
        lToErase.push_back(lIden);
      }
    }
  } else {
    /* find the exact match */
    mSubTimeFrame = std::make_unique<SubTimeFrame>(pStf.header().mId);

    for (auto& lKeyData : pStf.mData) {
      const DataIdentifier& lIden = lKeyData.first;
      if (lIden == mDataIdentifier) {
        // use the equipment identifier of the object
        mSubTimeFrame->mData[lIden] = std::move(lKeyData.second);
        lToErase.push_back(lIden);
      }
    }
  }

  // erase forked elements
  for (auto& lIden : lToErase)
    pStf.mData.erase(lIden);
}

std::unique_ptr<SubTimeFrame> DataIdentifierSplitter::split(SubTimeFrame& pStf, const DataIdentifier& pDataIdent)
{
  mDataIdentifier = pDataIdent;

  pStf.accept(*this);

  return std::move(mSubTimeFrame);
}
}
} /* o2::DataDistribution */
