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
