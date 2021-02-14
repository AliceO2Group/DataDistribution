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
}

void TfSchedulerDevice::PreRun()
{
  mDiscoveryConfig = std::make_unique<ConsulTfSchedulerService>(
    ProcessType::TfSchedulerService,
    Config::getEndpointOption(*GetConfig())
  );
}

void TfSchedulerDevice::PostRun()
{
  IDDLOG("Stopping the TfScheduler and exiting. partition_id={}", mPartitionId);

  // delete everything
  if (mSchedInstance) {
    mSchedInstance->stop();
  }
  mSchedInstance.reset();
  mPartitionId.clear();

  throw "Intentional exit";
}

void TfSchedulerDevice::ResetTask()
{
}

bool TfSchedulerDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);

  if (!mSchedInstance) {
    // check for new requests
    PartitionRequest lNewPartitionRequest;
    DDLOGF_RL(5000, DataDistSeverity::debug, "Checking for new partition creation requests.");
    if (mDiscoveryConfig->getNewPartitionRequest(lNewPartitionRequest)) {
      // new request
      IDDLOG("Request for starting a new partition. partition={}", lNewPartitionRequest.mPartitionId);

      // Create a new instance for the partition
      mPartitionId = lNewPartitionRequest.mPartitionId;
      mSchedInstance = std::make_unique<TfSchedulerInstanceHandler>(*this,
        std::string("0"), // TODO: add multiple schedulers
        lNewPartitionRequest
      );
      mSchedInstance->start();

      IDDLOG("Created new scheduler instance. partition={}", lNewPartitionRequest.mPartitionId);
    }
  }

  if (mSchedInstance) {
    if (mSchedInstance->isTerminated()) {
      std::this_thread::sleep_for(2000ms);
      return false; // -> PostRun() -> exit
    }
  }

  return true;
}

}
} /* namespace o2::DataDistribution */
