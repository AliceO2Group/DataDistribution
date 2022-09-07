// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

#ifndef ALICEO2_READOUT_EMULATOR_DEVICE_H_
#define ALICEO2_READOUT_EMULATOR_DEVICE_H_

#include "CruEmulator.h"

#include <ReadoutDataModel.h>
#include <Utilities.h>
#include <FmqUtilities.h>
#include <MemoryUtils.h>

#include <memory>
#include <deque>
#include <condition_variable>

namespace o2::DataDistribution
{

class ReadoutDevice : public DataDistDevice
{
 public:
  static constexpr const char* OptionKeyOutputChannelName = "output-channel-name";

  static constexpr const char* OptionKeyReadoutDataRegionSize = "data-shm-region-size";

  static constexpr const char* OptionKeyLinkIdOffset = "link-id-offset";

  static constexpr const char* OptionKeyCruSuperpageSize = "cru-superpage-size";

  static constexpr const char* OptionKeyOrbitsInTf = "orbits-in-tf";

  static constexpr const char* OptionKeyCruLinkCount = "cru-link-count";
  static constexpr const char* OptionKeyCruLinkBitsPerS = "cru-link-bits-per-s";

  /// Default constructor
  ReadoutDevice();

  /// Default destructor
  ~ReadoutDevice() override;

  void InitTask() final;
  void ResetTask() final;

 protected:
  bool ConditionalRun() final;
  void PreRun() final;
  void PostRun() final { };


  void InfoThread();
  void SendingThread();

  // data and Descriptor regions
  // must be here because NewUnmanagedRegionFor() is a method of FairMQDevice...
  std::unique_ptr<DataRegionAllocatorResource> mDataRegion;

  std::string mOutChannelName;
  std::size_t mDataRegionSize;

  std::size_t mLinkIdOffset;

  std::size_t mSuperpageSize;
  std::size_t mOrbitsInTf;
  std::size_t mDmaChunkSize;
  unsigned mCruLinkCount;
  std::uint64_t mCruLinkBitsPerS;

  std::shared_ptr<CruMemoryHandler> mCruMemoryHandler;

  std::vector<std::unique_ptr<CruLinkEmulator>> mCruLinks;

  // messages to send
  std::vector<fair::mq::MessagePtr> mDataBlockMsgs;
  std::thread mSendingThread;

  /// Observables
  std::thread mInfoThread;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_READOUT_EMULATOR_DEVICE_H_ */
