// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

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
  SubTimeFrameReadoutBuilder(FairMQDevice &pDev, FairMQChannel& pChan, bool pDplEnabled);

  void addHbFrames(const ReadoutSubTimeframeHeader& pHdr, std::vector<FairMQMessagePtr>&& pHbFrames);
  std::unique_ptr<SubTimeFrame> getStf();

 private:

  std::unique_ptr<SubTimeFrame> mStf;

  FairMQDevice& mDevice;
  FairMQChannel& mChan;
  bool mDplEnabled;

  std::unique_ptr<FMQUnsynchronizedPoolMemoryResource> mHeaderMemRes;

};

}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_BUILDER_H_ */
