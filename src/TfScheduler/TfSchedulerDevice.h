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

#ifndef ALICEO2_TF_SCHEDULER_DEVICE_H_
#define ALICEO2_TF_SCHEDULER_DEVICE_H_

#include "TfSchedulerInstance.h"

#include <ConfigConsul.h>

#include <Utilities.h>
#include <FmqUtilities.h>

#include <thread>
#include <chrono>

namespace o2::DataDistribution
{

class ConsulConfig;

class TfSchedulerDevice : public DataDistDevice
{
 public:
  /// Default constructor
  TfSchedulerDevice();

  /// Default destructor
  ~TfSchedulerDevice() override;

  void InitTask() final;
  void ResetTask() final;

 protected:
  void PreRun() final;
  void PostRun() final;
  bool ConditionalRun() final;

  /// Scheduler Instances
  std::string mPartitionId;

  std::chrono::steady_clock::time_point mStartTime;
  std::chrono::steady_clock::time_point mPartitionStartTime;

  std::unique_ptr<TfSchedulerInstanceHandler> mSchedInstance;

  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_DEVICE_H_ */
