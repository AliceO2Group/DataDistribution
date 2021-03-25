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

#include "StfSenderDevice.h"

#include <ConfigConsul.h>

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>
#include <SubTimeFrameDPL.h>

#include <options/FairMQProgOptions.h>

#include <chrono>
#include <thread>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

StfSenderDevice::StfSenderDevice()
  : DataDistDevice(),
    IFifoPipeline(ePipelineSize),
    mFileSink(*this, *this, eFileSinkIn, eFileSinkOut),
    mOutputHandler(*this),
    mRpcServer()
{
}

StfSenderDevice::~StfSenderDevice()
{
}

void StfSenderDevice::InitTask()
{
  DataDistLogger::SetThreadName("stfs-main");

  mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);
  mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);

  if (!mStandalone) {
    // Discovery
    mDiscoveryConfig = std::make_shared<ConsulStfSender>(ProcessType::StfSender, Config::getEndpointOption(*GetConfig()));

    auto& lStatus = mDiscoveryConfig->status();
    lStatus.mutable_info()->set_type(StfSender);
    lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
    lStatus.mutable_info()->set_process_id(Config::getIdOption(StfSender, *GetConfig()));
    lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*GetConfig()));

    // wait for "partition-id"
    while (!Config::getPartitionOption(*GetConfig())) {
      WDDLOG("TfBuilder waiting on 'discovery-partition' config parameter.");
      std::this_thread::sleep_for(1s);
    }

    lStatus.mutable_partition()->set_partition_id(*Config::getPartitionOption(*GetConfig()));
    mDiscoveryConfig->write();
  }

  try {
    GetChannel(mInputChannelName, 0);
  } catch (...) {
    EDDLOG("Requested input channel is not configured. input_chan={}", mInputChannelName);
    std::this_thread::sleep_for(1s); exit(-1);
  }

  // File sink
  if (!mFileSink.loadVerifyConfig(*GetConfig())) {
    std::this_thread::sleep_for(1s); exit(-1);
  }

  // check if any outputs enabled
  if (mStandalone && !mFileSink.enabled()) {
    WDDLOG("Running in standalone mode and with STF file sink disabled. Data will be lost.");
  }
}

void StfSenderDevice::PreRun()
{
  mRunning = true;

  if (!mStandalone) {
    // Start output handler
    mOutputHandler.start(mDiscoveryConfig);

    // start the RPC server after output
    int lRpcRealPort = 0;
    auto& lStatus = mDiscoveryConfig->status();
    mRpcServer.start(&mOutputHandler, lStatus.info().ip_address(), lRpcRealPort);
    lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));
    mDiscoveryConfig->write();

    // contact the scheduler on gRPC
    while (!mTfSchedulerRpcClient.start(mDiscoveryConfig)) {
      // try to reach the scheduler unless we should exit
      if (!IsReadyOrRunningState()) {
        return;
      }
      std::this_thread::sleep_for(250ms);
    }
  }

  // start file sink
  if (mFileSink.enabled()) {
    mFileSink.start();
  }

  // Info thread
  mInfoThread = create_thread_member("stfs_info", &StfSenderDevice::InfoThread, this);

  // start the receiver thread
  mReceiverThread = create_thread_member("stfs_recv", &StfSenderDevice::StfReceiverThread, this);

  // update running state
  auto& lStatus = mDiscoveryConfig->status();
  lStatus.mutable_info()->set_process_state(BasicInfo::RUNNING);
  mDiscoveryConfig->write();
}

void StfSenderDevice::PostRun()
{
  // update running state
  auto& lStatus = mDiscoveryConfig->status();
  lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
  mDiscoveryConfig->write();

  // Stop the pipeline
  stopPipeline();

  mRunning = false;

  // stop the receiver thread
  if (mReceiverThread.joinable()) {
    mReceiverThread.join();
  }

  // stop file sink
  if (mFileSink.enabled()) {
    mFileSink.stop();
  }

  // wait the Info thread, before closing mTfSchedulerRpcClient
  if (mInfoThread.joinable()) {
    mInfoThread.join();
  }

  if (!mStandalone) {
    // Stop the RPC server after output
    mRpcServer.stop();

    // Stop output handler
    mOutputHandler.stop();

    // Stop the Scheduler RPC client
    mTfSchedulerRpcClient.stop();
  }

  DDDLOG("PostRun() done.");
}

void StfSenderDevice::ResetTask()
{
  DDDLOG("ResetTask() done.");
}

void StfSenderDevice::StfReceiverThread()
{
  using hres_clock = std::chrono::high_resolution_clock;
  std::uint64_t lReceivedStfs = 0;

  auto& lInputChan = GetChannel(mInputChannelName, 0);

  // InterleavedHdrDataDeserializer lStfReceiver;
  DplToStfAdapter  lStfReceiver;
  std::unique_ptr<SubTimeFrame> lStf;

  const auto lStfStartTime = hres_clock::now();

  while (mRunning) {
    try {
      lStf = lStfReceiver.deserialize(lInputChan);
    } catch (const std::exception &e) {
      EDDLOG_RL(5000, "StfSender: received STF cannot be deserialized. what={}", e.what());
      continue;
    } catch (...) {
      EDDLOG_RL(5000, "StfSender: received STF cannot be deserialized. what=UNKNOWN");
      continue;
    }

    if (!lStf) {
      std::this_thread::sleep_for(10ms);
      continue; // timeout? try until the FMQFSM goes out of running
    }

    { // Input STF frequency
      const auto lStfDur = std::chrono::duration<double>(hres_clock::now() - lStfStartTime);
      mStfTimeSamples.Fill((float)lStfDur.count());
    }

    ++lReceivedStfs;
    DDDLOG_RL(5000, "StfSender received total of {} STFs.", lReceivedStfs);

    // get data size
    mStfSizeSamples.Fill(lStf->getDataSize());

    IDDLOG_RL(2000, "StfReceiverThread:: SubTimeFrame stf_id={} size={} unique_equip={}",
      lStf->header().mId, lStf->getDataSize(), lStf->getEquipmentIdentifiers().size());

    queue(eReceiverOut, std::move(lStf));
  }

  IDDLOG("StfSender received total of {} STFs.", lReceivedStfs);
  DDDLOG("Exiting StfReceiverThread.");
}

void StfSenderDevice::InfoThread()
{
  while (mRunning) {
    IDDLOG("StfSender: SubTimeFrame size_mean={} in_frequency_mean={:.4}",
      mStfSizeSamples.Mean(), mStfTimeSamples.MeanStepFreq());
    if (!standalone()) {
      const auto lCounters = mOutputHandler.getCounters();

      IDDLOG("StfSender: SubTimeFrame queued_stf_num={} queued_stf_size={} sending_stf_num={} sending_stf_size={} ",
          lCounters.mBufferedStfCnt, lCounters.mBufferedStfSize,
          lCounters.mBufferedStfCntSending, lCounters.mBufferedStfSizeSending);
    }

    std::this_thread::sleep_for(2s);
  }
  DDDLOG("Exiting Info thread.");
}

bool StfSenderDevice::ConditionalRun()
{
  if (mRpcServer.isTerminateRequested()) {
    IDDLOG_RL(10000, "DataDistribution partition is terminated.");
    return false; // trigger PostRun()
  }

  if (mRunning && !mStandalone && mDiscoveryConfig) {
    const auto lInfo = mDiscoveryConfig->status().info();
    mTfSchedulerRpcClient.HeartBeat(lInfo);
  }

  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);
  return true;
}

}
} /* namespace o2::DataDistribution */
