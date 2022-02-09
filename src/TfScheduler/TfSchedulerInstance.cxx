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

#include <DataDistLogger.h>
#include <DataDistMonitoring.h>

#include <condition_variable>
#include <stdexcept>

namespace o2
{
namespace DataDistribution
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
  DataDistMonitor::enable_datadist(1, mPartitionInfo.mPartitionId);

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

}
} /* o2::DataDistribution */
