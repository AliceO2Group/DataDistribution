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

#include "TfBuilderDevice.h"

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>

#include <ConfigConsul.h>
#include <SubTimeFrameDPL.h>

#include <chrono>
#include <thread>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

TfBuilderDevice::TfBuilderDevice()
  : DataDistDevice(),
    IFifoPipeline(eTfPipelineSize),
    mFileSink(*this, *this, eTfFileSinkIn, eTfFileSinkOut),
    mFileSource(*this, eTfFileSourceOut),
    mTfSizeSamples(),
    mTfFreqSamples()
{
}

TfBuilderDevice::~TfBuilderDevice()
{
  DDLOGF(fair::Severity::DEBUG, "TfBuilderDevice::~TfBuilderDevice()");
}

void TfBuilderDevice::Init()
{
  mMemI = std::make_unique<MemoryResources>(this->AddTransport(fair::mq::Transport::SHM));
}

void TfBuilderDevice::Reset()
{
  mMemI.reset();
}

void TfBuilderDevice::InitTask()
{
  DataDistLogger::SetThreadName("tfb-main");

  {
    mDplChannelName = GetConfig()->GetValue<std::string>(OptionKeyDplChannelName);
    mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
    mTfBufferSize = GetConfig()->GetValue<std::uint64_t>(OptionKeyTfMemorySize);
    mTfBufferSize <<= 20; /* input parameter is in MiB */

    mDiscoveryConfig = std::make_shared<ConsulTfBuilder>(ProcessType::TfBuilder,
      Config::getEndpointOption(*GetConfig()));

    auto &lStatus =  mDiscoveryConfig->status();
    lStatus.mutable_info()->set_type(TfBuilder);
    lStatus.mutable_info()->set_process_id(Config::getIdOption(*GetConfig()));
    lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*GetConfig()));

    mPartitionId = Config::getPartitionOption(*GetConfig());
    lStatus.mutable_partition()->set_partition_id(mPartitionId);

    // File sink
    if (!mFileSink.loadVerifyConfig(*(this->GetConfig()))) {
      throw "File Sink options";
      return;
    }

    mRpc = std::make_shared<TfBuilderRpcImpl>(mDiscoveryConfig);
    mFlpInputHandler = std::make_unique<TfBuilderInput>(*this, mRpc, eTfBuilderOut);
  }

  {
    auto &lStatus = mDiscoveryConfig->status();

    // finish discovery information: gRPC server
    int lRpcRealPort = 0;
    mRpc->initDiscovery(lStatus.info().ip_address(), lRpcRealPort);
    lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));

    if (! mDiscoveryConfig->write(true)) {
      DDLOGF(fair::Severity::ERROR, "Can not start TfBuilder. id={}", lStatus.info().process_id());
      DDLOGF(fair::Severity::ERROR, "Process with the same id already running? If not, clear the key manually.");
      throw "Discovery database error: this TfBuilder was/is already present.";
      return;
    }

    // Using DPL?
    if (mDplChannelName != "") {
      mDplEnabled = true;
      mStandalone = false;
      DDLOGF(fair::Severity::INFO, "Using DPL channel. channel_name={:s}", mDplChannelName);
    } else {
      mDplEnabled = false;
      mStandalone = true;
      DDLOGF(fair::Severity::INFO, "Not sending to DPL.");
    }

    // start the info thread
    mInfoThread = create_thread_member("tfb_info", &TfBuilderDevice::InfoThread, this);
  }
}

void TfBuilderDevice::PreRun()
{
  start();
}

bool TfBuilderDevice::start()
{
  // start all gRPC clients
  while (!mRpc->start(mTfBufferSize)) {
    // try to reach the scheduler unless we should exit
    if (IsRunningState() && NewStatePending()) {
      mShouldExit = true;
      return false;
    }

    std::this_thread::sleep_for(1s);
  }

  // we reached the scheduler instance, initialize everything else
  mRunning = true;
  auto lShmTransport = this->AddTransport(fair::mq::Transport::SHM);

  if (!mStandalone && dplEnabled()) {
    auto& lOutputChan = GetChannel(getDplChannelName(), 0);

    mTfBuilder = std::make_unique<TimeFrameBuilder>(MemI(), mTfBufferSize, 512 << 20 /* config */, dplEnabled());
    mTfDplAdapter = std::make_unique<StfToDplAdapter>(lOutputChan);
  }

  // start TF forwarding thread
  mTfFwdThread = create_thread_member("tfb_out", &TfBuilderDevice::TfForwardThread, this);
  // start file sink
  mFileSink.start();

  // Start input handlers
  if (!mFlpInputHandler->start(mDiscoveryConfig)) {
    mShouldExit = true;
    DDLOGF(fair::Severity::ERROR, "Could not initialize input connections. Exiting.");
    throw "Input connection error";
    return false;
  }

  // start file source
  mFileSource.start(MemI(), mStandalone ? false : mDplEnabled);

  return true;
}

void TfBuilderDevice::stop()
{
  mRpc->stopAcceptingTfs();

  if (mTfDplAdapter) {
    mTfDplAdapter->stop();
  }

  if (mTfBuilder) {
    mTfBuilder->stop();
  }

  mRunning = false;

  // Stop the pipeline
  stopPipeline();

  // stop output handlers
  mFlpInputHandler->stop(mDiscoveryConfig);
  // signal and wait for the output thread
  mFileSink.stop();
  // join on fwd thread
  if (mTfFwdThread.joinable()) {
    mTfFwdThread.join();
  }

  //wait for the info thread
  if (mInfoThread.joinable()) {
    mInfoThread.join();
  }

  // stop the RPCs
  mRpc->stop();

  mDiscoveryConfig.reset();

  DDLOGF(fair::Severity::DEBUG, "Reset() done... ");
}

void TfBuilderDevice::ResetTask()
{
  stop();
}

bool TfBuilderDevice::ConditionalRun()
{
  if (mShouldExit) {
    mRunning = false;
    return false;
  }

  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(250ms);
  return true;
}

void TfBuilderDevice::TfForwardThread()
{
  auto lFreqStartTime = std::chrono::high_resolution_clock::now();
  std::uint64_t lTfOutCnt = 0;

  while (mRunning) {
    std::unique_ptr<SubTimeFrame> lTf = dequeue(eTfFwdIn);
    if (!lTf) {
      DDLOGF(fair::Severity::INFO, "TfForwardThread(): Queue closed. Exiting... ");
      break;
    }

    const auto lTfId = lTf->header().mId;

    // MON: record frequency and size of TFs
    {
      mTfFreqSamples.Fill(
        1.0 / std::chrono::duration<double>(
                std::chrono::high_resolution_clock::now() - lFreqStartTime)
                .count());

      lFreqStartTime = std::chrono::high_resolution_clock::now();

      // size samples
      mTfSizeSamples.Fill(lTf->getDataSize());
    }

    if (!mStandalone) {
      try {
        lTfOutCnt++;
        DDLOGF_RL(1000, fair::Severity::INFO,
          "Forwarding a new TF to DPL. tf_id={} stf_size={:d} unique_equipments={:d} total={:d}",
          lTfId, lTf->getDataSize(), lTf->getEquipmentIdentifiers().size(), lTfOutCnt);

        if (dplEnabled()) {
          // adapt headers to include DPL processing header on the stack
          assert(mTfBuilder);
          mTfBuilder->adaptHeaders(lTf.get());

          // Send to DPL
          assert (mTfDplAdapter);
          mTfDplAdapter->sendToDpl(std::move(lTf));
        }
      } catch (std::exception& e) {
        if (IsRunningState()) {
          DDLOGF(fair::Severity::ERROR, "StfOutputThread: exception on send. exception_what={:s}", e.what());
        } else {
          DDLOGF(fair::Severity::INFO, "StfOutputThread: shutting down. exception_what={:s}", e.what());
        }
        break;
      }
    }

    // decrement the size used by the TF
    // TODO: move this close to the output channel send to have more precise accounting of free memory
    //       or, get the memory status directly from shm region
    mRpc->recordTfForwarded(lTfId);
  }

  DDLOGF(fair::Severity::INFO, "Exiting TF forwarding thread.");
}

void TfBuilderDevice::InfoThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {

    DDLOGF(fair::Severity::INFO, "Mean size of TimeFrames : {}", mTfSizeSamples.Mean());
    DDLOGF(fair::Severity::INFO, "Mean TimeFrame frequency: {}", mTfFreqSamples.Mean());
    DDLOGF(fair::Severity::INFO, "Number of queued TFs    : {}", getPipelineSize()); // current value

    std::this_thread::sleep_for(2s);
  }

  DDLOGF(fair::Severity::DEBUG, "Exiting info thread...");
}

}
} /* namespace o2::DataDistribution */
