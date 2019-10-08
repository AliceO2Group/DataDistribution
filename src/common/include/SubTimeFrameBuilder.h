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

#include <vector>
#include <mutex>

class FairMQDevice;
class FairMQChannel;

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameReadoutBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameReadoutBuilder
{
 public:
  SubTimeFrameReadoutBuilder() = delete;
  SubTimeFrameReadoutBuilder(FairMQChannel& pChan, bool pDplEnabled);

  void addHbFrames(const o2::header::DataOrigin &pDataOrig,
    const o2::header::DataHeader::SubSpecificationType pSubSpecification,
    ReadoutSubTimeframeHeader& pHdr, std::vector<FairMQMessagePtr>&& pHbFrames);
  std::unique_ptr<SubTimeFrame> getStf();

  void setRdhSanityCheck(bool pVal) { mRdhSanityCheck = pVal; }
  void setRdh4FilterTrigger(bool pVal) { mRdh4FilterTrigger = pVal; }

 private:

  std::unique_ptr<SubTimeFrame> mStf;

  FairMQChannel& mChan;
  bool mDplEnabled;

  std::unique_ptr<FMQUnsynchronizedPoolMemoryResource> mHeaderMemRes;


  bool mRdhSanityCheck = false;
  bool mRdh4FilterTrigger = false;
};


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameFileBuilder
{
 public:
  SubTimeFrameFileBuilder() = delete;
  SubTimeFrameFileBuilder(FairMQChannel& pChan, bool pDplEnabled);

  void adaptHeaders(SubTimeFrame *pStf);

 private:

  FairMQChannel& mChan;
  bool mDplEnabled;

  std::unique_ptr<FMQUnsynchronizedPoolMemoryResource> mHeaderMemRes;
};

}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_BUILDER_H_ */
