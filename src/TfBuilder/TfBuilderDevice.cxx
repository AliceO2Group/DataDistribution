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
#include <Framework/SourceInfoHeader.h>

#include <chrono>
#include <thread>
#include <future>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

TfBuilderDevice::TfBuilderDevice()
  : DataDistDevice(),
    IFifoPipeline(eTfPipelineSize),
    mFileSink(*this, *this, eTfFileSinkIn, eTfFileSinkOut)
{
}

TfBuilderDevice::~TfBuilderDevice()
{
  DDDLOG("TfBuilderDevice::~TfBuilderDevice()");
}

void TfBuilderDevice::Init()
{
  mMemI = std::make_unique<SyncMemoryResources>(this->AddTransport(fair::mq::Transport::SHM));
}

void TfBuilderDevice::Reset()
{
  mMemI->stop();
  mMemI.reset();
}

void TfBuilderDevice::InitTask()
{
  DataDistLogger::SetThreadName("tfb-main");

  mDplChannelName = GetConfig()->GetValue<std::string>(OptionKeyDplChannelName);
  mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
  mTfBufferSize = GetConfig()->GetValue<std::uint64_t>(OptionKeyTfMemorySize);
  mTfBufferSize <<= 20; /* input parameter is in MiB */

  // start monitoring
  DataDistMonitor::start_datadist(o2::monitoring::tags::Value::TfBuilder, GetConfig()->GetProperty<std::string>("monitoring-backend"));
  DataDistMonitor::set_interval(GetConfig()->GetValue<float>("monitoring-interval"));
  DataDistMonitor::set_log(GetConfig()->GetValue<bool>("monitoring-log"));

  // Using DPL?
  if (mDplChannelName != "") {
    mDplEnabled = true;
    mStandalone = false;
    IDDLOG("Using DPL channel. channel_name={}", mDplChannelName);
  } else {
    mDplEnabled = false;
    mStandalone = true;
    IDDLOG("Not sending to DPL.");
  }

  // start buffer creation in an async thread
  mTfBuilder = std::make_unique<TimeFrameBuilder>(MemI(), dplEnabled());
  std::promise<bool> lBuffersAllocated;
  std::future<bool> lBuffersAllocatedFuture = lBuffersAllocated.get_future();

  std::thread([&]{
    using hres_clock = std::chrono::high_resolution_clock;
    const auto lBufferStart = hres_clock::now();

    try {
      mTfBuilder->allocate_memory(mTfBufferSize, std::size_t(2048) << 20);
    } catch (std::exception &e) {
      IDDLOG("InitTask::MemorySegment allocation failed. what={}", e.what());
      // pass the failure
      lBuffersAllocated.set_value_at_thread_exit(false);
      return;
    }

    const auto lElapsed = std::chrono::duration<double>(hres_clock::now() - lBufferStart).count();
    IDDLOG("InitTask::MemorySegment allocated. success={} duration={:.3}", MemI().running(), lElapsed);

    // pass the result
    lBuffersAllocated.set_value_at_thread_exit(MemI().running());
  }).detach();

  mDiscoveryConfig = std::make_shared<ConsulTfBuilder>(ProcessType::TfBuilder, Config::getEndpointOption(*GetConfig()));

  auto &lStatus =  mDiscoveryConfig->status();
  lStatus.mutable_info()->set_type(TfBuilder);
  lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
  lStatus.mutable_info()->set_process_id(Config::getIdOption(TfBuilder, *GetConfig()));
  lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*GetConfig()));

  // wait for "partition-id"
  while (!Config::getPartitionOption(*GetConfig())) {
    WDDLOG("TfBuilder waiting on 'discovery-partition' config parameter.");
    std::this_thread::sleep_for(1s);
  }
  mPartitionId = Config::getPartitionOption(*GetConfig()).value_or("INVALID_PARTITION");
  lStatus.mutable_partition()->set_partition_id(mPartitionId);

  // File sink
  if (!mFileSink.loadVerifyConfig(*(this->GetConfig()))) {

    throw "File Sink options";
    return;
  }

  mRpc = std::make_shared<TfBuilderRpcImpl>(mDiscoveryConfig, *mMemI);
  mFlpInputHandler = std::make_unique<TfBuilderInput>(*this, mRpc, eTfBuilderOut);

  {
    // finish discovery information: gRPC server
    int lRpcRealPort = 0;
    mRpc->initDiscovery(lStatus.info().ip_address(), lRpcRealPort);
    lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));

    if (! mDiscoveryConfig->write(true)) {
      EDDLOG("Can not start TfBuilder. id={}", lStatus.info().process_id());
      EDDLOG("Process with the same id already running? If not, clear the key manually.");
      throw "Discovery database error: this TfBuilder was/is already present.";
      return;
    }
  }

  // start the task
  if (!start()) {
    mShouldExit = true;
    throw std::runtime_error("Aborting InitTask(). Cannot configure.");
  }

  // wait for the memory allocation and registration to finish
  lBuffersAllocatedFuture.wait();
  if (!lBuffersAllocatedFuture.get()) {
    EDDLOG("InitTask::MemorySegment allocation failed. Exiting...");
    throw "InitTask::MemorySegment allocation failed. Exiting...";
    return;
  }

  DDDLOG("InitTask completed.");
}

void TfBuilderDevice::PreRun()
{
  // update running state
  auto& lStatus = mDiscoveryConfig->status();
  lStatus.mutable_info()->set_process_state(BasicInfo::RUNNING);
  mDiscoveryConfig->write();

  // make directory for the file sink
  mFileSink.makeDirectory();

  // enable monitoring
  DataDistMonitor::enable_datadist(DataDistLogger::sRunNumber, mPartitionId);

  DDMON("tfbuilder", "running", 1);

  IDDLOG("Entering running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

bool TfBuilderDevice::start()
{
  while (!mRpc->start(mTfBufferSize)) {
    // check if should stop looking for TfScheduler
    if (mRpc->isTerminateRequested()) {
      mShouldExit = true;
      return false;
    }

    // try to reach the scheduler unless we should exit
    if (IsRunningState() && NewStatePending()) {
      mShouldExit = true;
      return false;
    }

    std::this_thread::sleep_for(1s);
  }

  // we reached the scheduler instance, initialize everything else
  mRunning = true;

  if (!mStandalone && dplEnabled()) {
    auto& lOutputChan = GetChannel(getDplChannelName(), 0);
    mTfDplAdapter = std::make_unique<StfToDplAdapter>(lOutputChan);
  }

  // start TF forwarding thread
  mTfFwdThread = create_thread_member("tfb_out", &TfBuilderDevice::TfForwardThread, this);
  // start file sink
  mFileSink.start();

  // Start input handlers
  if (!mFlpInputHandler->start(mDiscoveryConfig)) {
    mShouldExit = true;
    EDDLOG("Could not initialize input connections. Exiting.");
    return false;
  }

  return true;
}

void TfBuilderDevice::stop()
{
  if (mRpc) {
    mRpc->stopAcceptingTfs();
  }

  if (mTfDplAdapter) {
    mTfDplAdapter->stop();
  }

  mRunning = false;
  DDDLOG("TfBuilderDevice::stop(): mRunning is false.");

  // Stop the pipeline
  stopPipeline();

  // stop output handlers
  if (mFlpInputHandler) {
    mFlpInputHandler->stop(mDiscoveryConfig);
  }
  DDDLOG("TfBuilderDevice::stop(): Input handler stopped.");
  // signal and wait for the output thread
  mFileSink.stop();
  // join on fwd thread
  if (mTfFwdThread.joinable()) {
    mTfFwdThread.join();
  }
  DDDLOG("TfBuilderDevice::stop(): Forward thread stopped.");

  // stop the RPCs
  if (mRpc) {
    mRpc->stop();
  }
  DDDLOG("TfBuilderDevice::stop(): RPC clients stopped.");

  mDiscoveryConfig.reset();

  // memory resource is last to destruct
  if (mTfBuilder) {
    mTfBuilder->stop();
  }

  // stop monitoring
  DataDistMonitor::stop_datadist();

  DDDLOG("TfBuilderDevice() stopped... ");
}

void TfBuilderDevice::ResetTask()
{
  stop();
  DDDLOG("ResetTask()");
}

// Get here when ConditionalRun returns false
void TfBuilderDevice::PostRun()
{
  DDMON("tfbuilder", "running", 0);

  // update running state
  auto& lStatus = mDiscoveryConfig->status();
  lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
  mDiscoveryConfig->write();

  // disable monitoring
  DataDistMonitor::disable_datadist();

  IDDLOG("Exiting running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

bool TfBuilderDevice::ConditionalRun()
{
  if (mShouldExit || mRpc->isTerminateRequested()) {
    // mRunning = false;
    return false;
  }
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(250ms);

  {
    static unsigned sRateLimit_Monitoring = 0;
    if ((sRateLimit_Monitoring++ % 128) == 0) {
      DDMON("tfbuilder", "running", 1);
    }
  }
  return true;
}

void TfBuilderDevice::TfForwardThread()
{
  using hres_clock = std::chrono::high_resolution_clock;
  auto lRateStartTime = hres_clock::now();
  std::uint64_t lTfOutCnt = 0;

  while (mRunning) {
    std::unique_ptr<SubTimeFrame> lTf = dequeue(eTfFwdIn);
    if (!lTf) {
      IDDLOG("TfForwardThread(): Queue closed. Exiting... ");
      break;
    }

    const auto lTfId = lTf->id();
    {
      const auto lStfDur = std::chrono::duration<double>(hres_clock::now() - lRateStartTime);
      lRateStartTime = hres_clock::now();
      DDMON("tfbuilder", "tf_output.id", lTfId);
      DDMON("tfbuilder", "tf_output.size", lTf->getDataSize());
      DDMON("tfbuilder", "tf_output.rate", 1.0 / lStfDur.count());
    }

    if (!mStandalone) {
      try {
        lTfOutCnt++;
        IDDLOG_RL(5000, "Forwarding a new TF to DPL. tf_id={} stf_size={:d} unique_equipments={:d} total={:d}",
          lTfId, lTf->getDataSize(), lTf->getEquipmentIdentifiers().size(), lTfOutCnt);

        if (dplEnabled()) {
          // adapt headers to include DPL processing header on the stack
          assert(mTfBuilder);
          TfBuilderI().adaptHeaders(lTf.get());

          // Send to DPL
          assert (mTfDplAdapter);
          mTfDplAdapter->sendToDpl(std::move(lTf));
        }
      } catch (std::exception& e) {
        if (IsRunningState()) {
          EDDLOG("StfOutputThread: exception on send. exception_what={:s}", e.what());
        } else {
          IDDLOG("StfOutputThread: shutting down. exception_what={:s}", e.what());
        }
        break;
      }
    }

    // decrement the size used by the TF
    // TODO: move this close to the output channel send to have more precise accounting of free memory
    //       or, get the memory status directly from shm region
    mRpc->recordTfForwarded(lTfId);
  }

  // leaving the output thread, send end of the stream info
  if (!mStandalone && dplEnabled()) {
    o2::framework::SourceInfoHeader lDplExitHdr;
    lDplExitHdr.state = o2::framework::InputChannelState::Completed;
    auto lDoneStack = o2::header::Stack(
      o2::header::DataHeader(o2::header::gDataDescriptionInfo, o2::header::gDataOriginAny, 0, 0),
      o2::framework::DataProcessingHeader(),
      lDplExitHdr
    );

    // Send a multipart
    auto& lOutputChan = GetChannel(getDplChannelName(), 0);
    FairMQParts lCompletedMsg;
    auto lNoFree = [](void*, void*) { /* stack */ };
    lCompletedMsg.AddPart(lOutputChan.NewMessage(lDoneStack.data(), lDoneStack.size(), lNoFree));
    lCompletedMsg.AddPart(lOutputChan.NewMessage());
    lOutputChan.Send(lCompletedMsg);

    IDDLOG("Source Completed message sent to DPL.");
    // NOTE: no guarantees this will be sent out
    std::this_thread::sleep_for(2s);
  }

  DDDLOG("Exiting TF forwarding thread.");
}

} /* namespace o2::DataDistribution */
