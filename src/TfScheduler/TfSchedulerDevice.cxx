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

#include "TfSchedulerDevice.h"
#include "TfSchedulerInstance.h"
#include "TfSchedulerInstanceRpc.h"

#include <ConfigConsul.h>

#include <fairmq/ProgOptions.h>

#include <chrono>
#include <thread>
#include <tuple>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

TfSchedulerDevice::TfSchedulerDevice()
  : DataDistDevice()
{
}

TfSchedulerDevice::~TfSchedulerDevice()
{
}

void TfSchedulerDevice::InitTask()
{
  DataDistLogger::SetThreadName("tfs-main");

  mDiscoveryConfig = std::make_shared<ConsulTfScheduler>(ProcessType::TfScheduler, Config::getEndpointOption(*GetConfig()));

  mPartitionId = Config::getPartitionOption(*GetConfig()).value_or("");
  if (mPartitionId.empty()) {
    WDDLOG("TfScheduler 'discovery-partition' parameter not set during InitTask(). Exiting.");
    ChangeStateOrThrow(fair::mq::Transition::ErrorFound);
    return;
  }

  mStartTime = std::chrono::steady_clock::now();


  while (!mSchedInstance) {
    // prevent infinite looping. Look for the specified request for 5min and exit
    if (since<std::chrono::minutes>(mStartTime) > 30.0) {
      IDDLOG("Partition request not found. Exiting. partition={}", mPartitionId);
      ChangeStateOrThrow(fair::mq::Transition::ErrorFound);
      return;
    }

    if (NewStatePending()) {
      IDDLOG("Exiting on request from control system.");
      ChangeStateOrThrow(fair::mq::Transition::ErrorFound);
      return;
    }

    // check for new requests
    PartitionRequest lNewPartitionRequest;
    DDDLOG_RL(5000, "Checking for new partition creation requests.");
    if (mDiscoveryConfig->getNewPartitionRequest(mPartitionId, lNewPartitionRequest)) {
      // new request
      IDDLOG_RL(5000, "Request for starting a new partition. partition={}", lNewPartitionRequest.mPartitionId);

      // Create a new instance for the partition
      mPartitionId = lNewPartitionRequest.mPartitionId;
      mSchedInstance = std::make_unique<TfSchedulerInstanceHandler>(*this,
        mDiscoveryConfig,
        std::string("0"), // multiple schedulers?
        lNewPartitionRequest
      );
      if (mSchedInstance->start()) {
        IDDLOG("Created new scheduler instance. partition={}", lNewPartitionRequest.mPartitionId);
        mPartitionStartTime = std::chrono::steady_clock::now();
      } else {
        EDDLOG("Failed to create new scheduler instance. partition={}", lNewPartitionRequest.mPartitionId);
        ChangeStateOrThrow(fair::mq::Transition::ErrorFound);
        return;
      }
    }
  }
}

void TfSchedulerDevice::PreRun()
{
}

void TfSchedulerDevice::PostRun()
{
  ChangeStateOrThrow(fair::mq::Transition::End);
}

void TfSchedulerDevice::ResetTask()
{
  IDDLOG("Stopping the TfScheduler and exiting. partition_id={}", mPartitionId);

  // delete everything
  if (mSchedInstance) {
    mSchedInstance->stop();
  }
  mSchedInstance.reset();
  mPartitionId.clear();

  // throw "Intentional exit";
  ChangeStateOrThrow(fair::mq::Transition::End);
}

bool TfSchedulerDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);

  if (mSchedInstance) {
    if (mSchedInstance->isTerminated() || mSchedInstance->isError()) {
      ChangeStateOrThrow(fair::mq::Transition::End);
      return false; // -> PostRun() -> exit
    }

    // prevent indefinite processes. Stop after 120 hours
    if (since<std::chrono::hours>(mPartitionStartTime) > 120.0) {
      IDDLOG("Scheduler closing on timeout. Missing terminate request? Exiting. partition={}", mPartitionId);
      return false;
    }
  } else {
    return false;
  }

  return true;
}

} /* namespace o2::DataDistribution */
