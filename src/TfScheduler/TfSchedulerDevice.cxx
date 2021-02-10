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

#include "TfSchedulerDevice.h"
#include "TfSchedulerInstance.h"
#include "TfSchedulerInstanceRpc.h"

#include <ConfigConsul.h>

#include <options/FairMQProgOptions.h>

#include <chrono>
#include <thread>
#include <tuple>

namespace o2
{
namespace DataDistribution
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

  // Discovery
  mDiscoveryConfig = std::make_shared<ConsulTfSchedulerService>(ProcessType::TfSchedulerService, Config::getEndpointOption(*GetConfig()));

  auto &lStatus = mDiscoveryConfig->status();

  lStatus.mutable_info()->set_type(TfSchedulerService);
  lStatus.mutable_info()->set_process_id(Config::getIdOption(*GetConfig()));
  lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*GetConfig()));

  mDiscoveryConfig->write();

  // start the service thread
  mServiceThread = create_thread_member("sched_service", &TfSchedulerDevice::TfSchedulerServiceThread, this);
}

void TfSchedulerDevice::PreRun()
{

}

void TfSchedulerDevice::ResetTask()
{
  // stop the scheduler service
  if (mServiceThread.joinable()) {
    mServiceThread.join();
  }

  // stop all instances
  for (auto &lInstIt : mSchedulerInstances) {
    lInstIt.second->stop();
  }

  mSchedulerInstances.clear();


  DDDLOG("ResetTask() done.");
}

bool TfSchedulerDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);

  // NOTE: Not using Run or ConditionalRun lets us put teardown in PostRun()
  return true;
}


void TfSchedulerDevice::TfSchedulerServiceThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {

    // check for new requests
    PartitionRequest lNewPartitionRequest;
    DDLOGF_RL(2000, DataDistSeverity::debug, "Checking for new partition creation requests.");
    if (mDiscoveryConfig->getNewPartitionRequest(lNewPartitionRequest)) {
      // new request
      IDDLOG("Request for starting a new partition. partition={}", lNewPartitionRequest.mPartitionId);

      // check if we already have instance for the requested partition
      if (mSchedulerInstances.count(lNewPartitionRequest.mPartitionId) == 0) {

        // Create a new instance for the partition
        auto [lNewInstIt, lEmplaced ] = mSchedulerInstances.emplace(
          lNewPartitionRequest.mPartitionId,
          std::make_unique<TfSchedulerInstanceHandler>(*this,
            mDiscoveryConfig->status().info().process_id() + "-" + lNewPartitionRequest.mPartitionId,
            lNewPartitionRequest
          ) // value
        );

        if (lEmplaced) {
          auto &lNewInstance = lNewInstIt->second;
          lNewInstance->start();
        }
        IDDLOG("Created new scheduler instance. partition={}", lNewPartitionRequest.mPartitionId);
        break; // Only service one partition per process
      } else {
        EDDLOG("Scheduler instance already exists. partition={}",lNewPartitionRequest.mPartitionId);
      }
    }

    std::this_thread::sleep_for(1000ms);
  }

  DDDLOG("Exiting TfSchedulerServiceThread.");
}

}
} /* namespace o2::DataDistribution */
