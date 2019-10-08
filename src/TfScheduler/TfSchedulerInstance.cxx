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

#include "TfSchedulerInstance.h"

#include <ConfigConsul.h>

#include <FairMQLogger.h>

#include <condition_variable>
#include <stdexcept>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

TfSchedulerInstanceHandler::TfSchedulerInstanceHandler(DataDistDevice& pDev,
  const std::string &pProcessId,
  const PartitionRequest &pPartitionRequest)
: mDevice(pDev),
  mPartitionInfo(pPartitionRequest),
  mDiscoveryConfig(std::make_shared<ConsulTfSchedulerInstance>(ProcessType::TfSchedulerInstance, Config::getEndpointOption(*pDev.GetConfig()))),
  mRpcServer(mDiscoveryConfig, pPartitionRequest)
{
  auto &lStatus = mDiscoveryConfig->status();

  lStatus.mutable_info()->set_type(TfSchedulerInstance);
  lStatus.mutable_info()->set_process_id(pProcessId);
  lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*pDev.GetConfig()));
  lStatus.mutable_partition()->set_partition_id(mPartitionInfo.mPartitionId);

  lStatus.set_stf_sender_count(mPartitionInfo.mStfSenderIdList.size());
  for (const auto &lStfSenderId : mPartitionInfo.mStfSenderIdList) {
    lStatus.add_stf_sender_id_list(lStfSenderId);
  }

  // start RPC server
  int lRpcRealPort = 0;
  mRpcServer.initDiscovery(lStatus.info().ip_address(), lRpcRealPort);
  lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));

  mDiscoveryConfig->write();

  LOG(DEBUG) << "Initialized new TfSchedulerInstance for partition " << mPartitionInfo.mPartitionId;
}

void TfSchedulerInstanceHandler::start()
{
  // create scheduler thread
  mSchedulerInstanceThread = std::thread(&TfSchedulerInstanceHandler::TfSchedulerInstanceThread, this);

  // start rpc processing
  mRpcServer.start();

  LOG(DEBUG) << "Started new TfSchedulerInstance for partition " << mPartitionInfo.mPartitionId;
}

void TfSchedulerInstanceHandler::stop()
{
  mRpcServer.stop();

  // stop the scheduler
  if (mSchedulerInstanceThread.joinable()) {
    mSchedulerInstanceThread.join();
  }
}

bool TfSchedulerInstanceHandler::running() const
{
  return mDevice.IsRunningState();
}

void TfSchedulerInstanceHandler::TfSchedulerInstanceThread()
{
  LOG(INFO) << "Starting a TfSchedulerInstanceHandler thread...";
  // wait for the device to go into RUNNING state
  mDevice.WaitForRunningState();

  while (running()) {

    std::this_thread::sleep_for(500ms);
  }

  LOG(INFO) << "Exiting TfSchedulerInstanceHandler thread...";
}

}
} /* o2::DataDistribution */
