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

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

StfSenderDevice::StfSenderDevice()
  : DataDistDevice()
{
}

StfSenderDevice::~StfSenderDevice()
{
}

void StfSenderDevice::Init()
{
  DDDLOG("StfSenderDevice::Init()");
  mI = std::make_unique<StfSenderInstance>();

  I().mFileSink = std::make_unique<SubTimeFrameFileSink>(*this, *mI, eFileSinkIn, eFileSinkOut);
  I().mOutputHandler = std::make_unique<StfSenderOutput>(*this, *mI);
}

void StfSenderDevice::Reset()
{
  DDDLOG("StfBuilderDevice::Reset()");
  // clear all Stfs from the pipeline before the transport is deleted
  if (mI) {
    I().stopPipeline();
    I().clearPipeline();
    mI.reset();
  }
}

void StfSenderDevice::InitTask()
{
  DataDistLogger::SetThreadName("stfs-main");

  I().mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);
  I().mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);

  if (!standalone()) {
    // Discovery
    I().mDiscoveryConfig = std::make_shared<ConsulStfSender>(ProcessType::StfSender, Config::getEndpointOption(*GetConfig()));

    auto& lStatus = I().mDiscoveryConfig->status();
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
    I().mDiscoveryConfig->write();
  }

  try {
    GetChannel(I().mInputChannelName, 0);
  } catch (...) {
    EDDLOG("Requested input channel is not configured. input_chan={}", I().mInputChannelName);
    std::this_thread::sleep_for(1s); exit(-1);
  }

  // File sink
  if (!I().mFileSink->loadVerifyConfig(*GetConfig())) {
    std::this_thread::sleep_for(1s); exit(-1);
  }

  // check if any outputs enabled
  if (standalone() && !I().mFileSink->enabled()) {
    WDDLOG("Running in standalone mode and with STF file sink disabled. Data will be lost.");
  }

  {
    I().mRunning = true;

    if (!standalone()) {
      // Start output handler
      I().mOutputHandler->start(I().mDiscoveryConfig);

      // start the RPC server after output
      int lRpcRealPort = 0;
      auto& lStatus = I().mDiscoveryConfig->status();
      I().mRpcServer.start(I().mOutputHandler.get(), lStatus.info().ip_address(), lRpcRealPort);
      lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));
      I().mDiscoveryConfig->write();

      // contact the scheduler on gRPC
      while (!I().mTfSchedulerRpcClient.start(I().mDiscoveryConfig)) {
        std::this_thread::sleep_for(250ms);
      }
    }

    // start file sink
    if (I().mFileSink->enabled()) {
      I().mFileSink->start();
    }

    // Info thread
    I().mInfoThread = create_thread_member("stfs_info", &StfSenderDevice::InfoThread, this);

    // start the receiver thread
    I().mReceiverThread = create_thread_member("stfs_recv", &StfSenderDevice::StfReceiverThread, this);
  }
}

void StfSenderDevice::PreRun()
{
  // update running state
  if (!standalone() && I().mDiscoveryConfig) {
    auto& lStatus = I().mDiscoveryConfig->status();
    lStatus.mutable_info()->set_process_state(BasicInfo::RUNNING);
    I().mDiscoveryConfig->write();
  }

  // make directory for file sink
  if (I().mFileSink) {
    I().mFileSink->makeDirectory();
  }

  // start accepting data
  I().mAcceptingData = true;

  IDDLOG("Entering running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

void StfSenderDevice::PostRun()
{
  // stop accepting data
  I().mAcceptingData = false;

  // update running state
  if (!standalone() && I().mDiscoveryConfig) {
    auto& lStatus = I().mDiscoveryConfig->status();
    lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
    I().mDiscoveryConfig->write();
  }

  IDDLOG("Exiting running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

void StfSenderDevice::ResetTask()
{
  // Stop the pipeline
  I().stopPipeline();
  I().clearPipeline();

  I().mRunning = false;

  // stop the receiver thread
  if (I().mReceiverThread.joinable()) {
    I().mReceiverThread.join();
  }

  // stop file sink
  if (I().mFileSink->enabled()) {
    I().mFileSink->stop();
  }

  // wait the Info thread, before closing mTfSchedulerRpcClient
  if (I().mInfoThread.joinable()) {
    I().mInfoThread.join();
  }

  if (!standalone()) {
    // Stop the RPC server after output
    I().mRpcServer.stop();

    // Stop output handler
    I().mOutputHandler->stop();

    // Stop the Scheduler RPC client
    I().mTfSchedulerRpcClient.stop();
  }

  DDDLOG("ResetTask() done.");
}

void StfSenderDevice::StfReceiverThread()
{
  using hres_clock = std::chrono::high_resolution_clock;
  std::uint64_t lReceivedStfs = 0;

  auto& lInputChan = GetChannel(I().mInputChannelName, 0);

  // InterleavedHdrDataDeserializer lStfReceiver;
  DplToStfAdapter  lStfReceiver;
  std::unique_ptr<SubTimeFrame> lStf;

  const auto lStfStartTime = hres_clock::now();

  while (running()) {
    try {
      lStf = lStfReceiver.deserialize(lInputChan, acceptingData());
    } catch (const std::exception &e) {
      EDDLOG_RL(5000, "StfSender: received STF cannot be deserialized. what={}", e.what());
      continue;
    } catch (...) {
      EDDLOG_RL(5000, "StfSender: received STF cannot be deserialized. what=UNKNOWN");
      continue;
    }

    if (!acceptingData()) {
      if (lStf) {
        WDDLOG_RL(1000, "StfSender: received STF but not in the running state.");
      }
      continue;
    }

    if (!lStf) {
      std::this_thread::sleep_for(10ms);
      continue; // timeout? try until the FMQFSM goes out of running
    }

    { // Input STF frequency
      const auto lStfDur = std::chrono::duration<double>(hres_clock::now() - lStfStartTime);
      I().mStfTimeSamples.Fill((float)lStfDur.count());
    }

    ++lReceivedStfs;
    DDDLOG_RL(5000, "StfSender received total of {} STFs.", lReceivedStfs);

    // get data size
    I().mStfSizeSamples.Fill(lStf->getDataSize());

    IDDLOG_RL(2000, "StfReceiverThread:: SubTimeFrame stf_id={} size={} unique_equip={}",
      lStf->header().mId, lStf->getDataSize(), lStf->getEquipmentIdentifiers().size());

    I().queue(eReceiverOut, std::move(lStf));
  }

  IDDLOG("StfSender received total of {} STFs.", lReceivedStfs);
  DDDLOG("Exiting StfReceiverThread.");
}

void StfSenderDevice::InfoThread()
{
  while (running()) {
    IDDLOG("StfSender: SubTimeFrame size_mean={} in_frequency_mean={:.4}",
      I().mStfSizeSamples.Mean(), I().mStfTimeSamples.MeanStepFreq());
    if (!standalone()) {
      const auto lCounters = I().mOutputHandler->getCounters();

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
  if (I().mRpcServer.isTerminateRequested()) {
    IDDLOG_RL(10000, "DataDistribution partition is terminated.");
    return false; // trigger PostRun()
  }

  if (running() && !standalone() && I().mDiscoveryConfig) {
    const auto lInfo = I().mDiscoveryConfig->status().info();
    I().mTfSchedulerRpcClient.HeartBeat(lInfo);
  }

  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);
  return true;
}

} /* namespace o2::DataDistribution */
