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

#include "TfSchedulerInstance.h"

#include <ConfigConsul.h>

#include <DataDistLogger.h>
#include <DataDistMonitoring.h>

#include <condition_variable>
#include <stdexcept>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

TfSchedulerInstanceHandler::TfSchedulerInstanceHandler(DataDistDevice& pDev,
  std::shared_ptr<ConsulTfScheduler> pConfig,
  const std::string &pProcessId,
  const PartitionRequest &pPartitionRequest)
: mPartitionInfo(pPartitionRequest),
  mDiscoveryConfig(pConfig),
  mRpcServer(mDiscoveryConfig, pPartitionRequest)
{
  auto &lStatus = mDiscoveryConfig->status();

  lStatus.mutable_info()->set_type(TfScheduler);
  lStatus.mutable_info()->set_process_state(BasicInfo::RUNNING);
  lStatus.mutable_info()->set_process_id(pProcessId);
  lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*pDev.GetConfig()));
  lStatus.mutable_partition()->set_partition_id(mPartitionInfo.mPartitionId);
  lStatus.set_allocated_partition_params(&mPartitionInfo.mParameters);

  lStatus.set_stf_sender_count(mPartitionInfo.mStfSenderIdList.size());
  for (const auto &lStfSenderId : mPartitionInfo.mStfSenderIdList) {
    lStatus.add_stf_sender_id_list(lStfSenderId);
  }
  lStatus.set_partition_state(PARTITION_CONFIGURING);

  // start RPC server
  int lRpcRealPort = 0;
  mRpcServer.initDiscovery(lStatus.info().ip_address(), lRpcRealPort);
  lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));

  mDiscoveryConfig->write();

  // start monitoring
  DataDistMonitor::start_datadist(o2::monitoring::tags::Value::TfScheduler, pDev.GetConfig()->GetProperty<std::string>("monitoring-backend"));
  DataDistMonitor::set_interval(pDev.GetConfig()->GetValue<float>("monitoring-interval"));
  DataDistMonitor::set_log(pDev.GetConfig()->GetValue<bool>("monitoring-log"));

  // enable monitoring
  DataDistMonitor::enable_datadist(0, mPartitionInfo.mPartitionId);

  DDDLOG("Initialized new TfScheduler. partition={}", mPartitionInfo.mPartitionId);
}

TfSchedulerInstanceHandler::~TfSchedulerInstanceHandler()
{
  stop();
}

bool TfSchedulerInstanceHandler::start()
{
  mRunning = true;
  // create scheduler thread
  mSchedulerInstanceThread = create_thread_member("sched_instance",
    &TfSchedulerInstanceHandler::TfSchedulerInstanceThread, this);

  // start rpc processing
  if (!mRpcServer.start()) {
    EDDLOG("Failed to reach all StfSenders. partition={}", mPartitionInfo.mPartitionId);
    return false;
  }

  DDDLOG("Started new TfScheduler. partition={}", mPartitionInfo.mPartitionId);
  return true;
}

void TfSchedulerInstanceHandler::stop()
{
  DDDLOG("TfSchedulerInstanceHandler::stop()");
  // stop monitoring
  DataDistMonitor::stop_datadist();

  mRpcServer.stop();

  mRunning = false;
  // stop the scheduler
  if (mSchedulerInstanceThread.joinable()) {
    mSchedulerInstanceThread.join();
  }
  DDDLOG("Stopped: TfScheduler");
}

void TfSchedulerInstanceHandler::TfSchedulerInstanceThread()
{
  DDDLOG("Starting a TfSchedulerInstanceHandler thread.");

  while (mRunning) {


    std::this_thread::sleep_for(1000ms);
  }

  DDDLOG("Exiting TfSchedulerInstanceHandler thread.");
}

} /* o2::DataDistribution */
