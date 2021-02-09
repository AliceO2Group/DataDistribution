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
  mMaxStfsInPipeline = GetConfig()->GetValue<std::int64_t>(OptionKeyMaxBufferedStfs);

  if (!mStandalone) {
    // Discovery
    mDiscoveryConfig = std::make_shared<ConsulStfSender>(ProcessType::StfSender, Config::getEndpointOption(*GetConfig()));

    auto& lStatus = mDiscoveryConfig->status();
    lStatus.mutable_info()->set_type(StfSender);
    lStatus.mutable_info()->set_process_id(Config::getIdOption(*GetConfig()));
    lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*GetConfig()));
    lStatus.mutable_partition()->set_partition_id(Config::getPartitionOption(*GetConfig()));
    mDiscoveryConfig->write();
  }

  try {
    GetChannel(mInputChannelName, 0);
  } catch (...) {
    EDDLOG("Requested input channel is not configured. input_chan={}", mInputChannelName);
    std::this_thread::sleep_for(1s); exit(-1);
  }

  // Buffering limitation
  if (mMaxStfsInPipeline > 0) {
    if (mMaxStfsInPipeline < 4) {
      mMaxStfsInPipeline = 4;
      IDDLOG("Max buffered SubTimeFrames limit increased to {}.", mMaxStfsInPipeline);
    }
    mPipelineLimit = true;
    WDDLOG("Max buffered SubTimeFrames limit is set to {}. Consider increasing it if data loss occurs.",
      mMaxStfsInPipeline);
  } else {
    mPipelineLimit = false;
    IDDLOG("Not imposing limits on number of buffered SubTimeFrames. Possibility of creating back-pressure.");
  }

  // File sink
  if (!mFileSink.loadVerifyConfig(*GetConfig())) {
    std::this_thread::sleep_for(1s); exit(-1);
  }

  // check if any outputs enabled
  if (mStandalone && !mFileSink.enabled()) {
    WDDLOG("Running in standalone mode and with STF file sink disabled. Data will be lost.");
  }

  // Info thread
  mInfoThread = create_thread_member("stfs_info", &StfSenderDevice::InfoThread, this);
}

void StfSenderDevice::PreRun()
{
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
  // start the receiver thread
  mReceiverThread = create_thread_member("stfs_recv", &StfSenderDevice::StfReceiverThread, this);
}

void StfSenderDevice::ResetTask()
{
  // Stop the pipeline
  stopPipeline();

  // stop the receiver thread
  if (mReceiverThread.joinable()) {
    mReceiverThread.join();
  }

  // stop file sink
  if (mFileSink.enabled()) {
    mFileSink.stop();
  }

  if (!mStandalone) {
    // start the RPC server after output
    mRpcServer.stop();

    // Stop output handler
    mOutputHandler.stop();

    // Stop the Scheduler RPC client
    mTfSchedulerRpcClient.stop();
  }

  // wait the Info thread
  if (mInfoThread.joinable()) {
    mInfoThread.join();
  }

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

  // wait for the device to go into RUNNING state
  WaitForRunningState();

  const auto lStfStartTime = hres_clock::now();

  while (IsRunningState()) {
    lStf = lStfReceiver.deserialize(lInputChan);

    if (!lStf) {
      std::this_thread::sleep_for(10ms);
      continue; // timeout? try until the FMQFSM goes out of running
    }

    { // Input STF frequency
      const auto lStfDur = std::chrono::duration<double>(hres_clock::now() - lStfStartTime);
      mStfTimeSamples.Fill((float)lStfDur.count());
    }

    ++lReceivedStfs;
    DDLOGF_RL(5000, DataDistSeverity::debug, "StfSender received total of {} STFs.", lReceivedStfs);

    // get data size
    mStfSizeSamples.Fill(lStf->getDataSize());

    DDLOGF_RL(2000, DataDistSeverity::info, "StfReceiverThread:: SubTimeFrame stf_id={} size={} unique_equip={}",
      lStf->header().mId, lStf->getDataSize(), lStf->getEquipmentIdentifiers().size());

    queue(eReceiverOut, std::move(lStf));
  }

  IDDLOG("StfSender received total of {} STFs.", lReceivedStfs);
  DDDLOG("Exiting StfReceiverThread.");
}

void StfSenderDevice::InfoThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {
    IDDLOG("SubTimeFrame size_mean={} in_frequency_mean={:.4} queued_stf={}",
      mStfSizeSamples.Mean(), mStfTimeSamples.MeanStepFreq(), mNumStfs);

    std::this_thread::sleep_for(2s);
  }
  DDDLOG("Exiting Info thread.");
}

bool StfSenderDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(1000ms);
  return true;
}
}
} /* namespace o2::DataDistribution */
