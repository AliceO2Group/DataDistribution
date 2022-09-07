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

#ifndef ALICEO2_TF_SCHEDULER_INSTANCE_H_
#define ALICEO2_TF_SCHEDULER_INSTANCE_H_

#include "TfSchedulerInstanceRpc.h"

#include <ConfigParameters.h>
#include <ConfigConsul.h>

#include <Utilities.h>
#include <FmqUtilities.h>

#include <fairmq/ProgOptionsFwd.h>

#include <vector>
#include <map>
#include <thread>

namespace o2::DataDistribution
{

class ConsulConfig;

class TfSchedulerInstanceHandler
{
 public:
  TfSchedulerInstanceHandler() = delete;
  TfSchedulerInstanceHandler(DataDistDevice& pDev,
    std::shared_ptr<ConsulTfScheduler> pConfig,
    const std::string &pProcessId,
    const PartitionRequest &pPartitionRequest);

  ~TfSchedulerInstanceHandler();

  bool start();
  void stop();

  void TfSchedulerInstanceThread();

  bool isTerminated() const { return mRpcServer.getPartitionState() == PartitionState::PARTITION_TERMINATED; }
  bool isError() const { return mRpcServer.getPartitionState() == PartitionState::PARTITION_ERROR; }

 private:
    /// Partiton info
  PartitionRequest mPartitionInfo;

  /// Discovery configuration
  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;

  /// Scheduler threads
  bool mRunning = false;
  std::thread mSchedulerInstanceThread;

  /// Rpc server and connection manager
  TfSchedulerInstanceRpcImpl mRpcServer;

};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_INSTANCE_H_ */
