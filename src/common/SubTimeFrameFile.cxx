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

#include "SubTimeFrameFile.h"

namespace o2
{
namespace DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileMeta
////////////////////////////////////////////////////////////////////////////////

const o2::header::DataDescription SubTimeFrameFileMeta::sDataDescFileSubTimeFrame{ "FILE_STF_META" };

std::ostream& operator<<(std::ostream& pStream, const SubTimeFrameFileMeta& pMeta)
{
  static_assert(std::is_standard_layout<SubTimeFrameFileMeta>::value,
                "SubTimeFrameFileMeta must be a std layout type.");

  // write DataHeader
  const o2::header::DataHeader lDataHeader = SubTimeFrameFileMeta::getDataHeader();
  pStream.write(reinterpret_cast<const char*>(&lDataHeader), sizeof(o2::header::DataHeader));
  // write the meta

  return pStream.write(reinterpret_cast<const char*>(&pMeta), sizeof(SubTimeFrameFileMeta));
}

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileDataIndex
////////////////////////////////////////////////////////////////////////////////

const o2::header::DataDescription SubTimeFrameFileDataIndex::sDataDescFileStfDataIndex{ "FILE_STF_INDEX" };

std::ostream& operator<<(std::ostream& pStream, const SubTimeFrameFileDataIndex& pIndex)
{
  static_assert(std::is_standard_layout<SubTimeFrameFileDataIndex::DataIndexElem>::value,
                "SubTimeFrameFileDataIndex::DataIndexElem must be a std layout type.");

  // write DataHeader
  const o2::header::DataHeader lDataHeader = pIndex.getDataHeader();
  pStream.write(reinterpret_cast<const char*>(&lDataHeader), sizeof(o2::header::DataHeader));

  // write the index
  return pStream.write(reinterpret_cast<const char*>(pIndex.mDataIndex.data()),
                       pIndex.mDataIndex.size() * sizeof(SubTimeFrameFileDataIndex::DataIndexElem));
}
}
} /* o2::DataDistribution */
