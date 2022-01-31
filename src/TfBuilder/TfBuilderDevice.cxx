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
  mTfDataRegionSize = GetConfig()->GetValue<std::uint64_t>(OptionKeyTfDataRegionSize);
  mTfDataRegionSize <<= 20; /* input parameter is in MiB */
  mTfDataRegionId = GetConfig()->GetValue<std::uint16_t>(OptionKeyTfDataRegionId);


  if (mTfDataRegionId.value() == std::uint16_t(~0)) {
    mTfDataRegionId.reset();
  }

  mTfHdrRegionSize = GetConfig()->GetValue<std::uint64_t>(OptionKeyTfHdrRegionSize);
  mTfHdrRegionSize <<= 20; /* input parameter is in MiB */
  mTfHdrRegionId = GetConfig()->GetValue<std::uint16_t>(OptionKeyTfHdrRegionId);

  if (mTfHdrRegionId.value() == std::uint16_t(~0)) {
    mTfHdrRegionId = std::nullopt;
  }

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
      mTfBuilder->allocate_memory(mTfDataRegionSize, mTfDataRegionId, mTfHdrRegionSize, mTfHdrRegionId);
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

  // set max number of request in flight
  mRpc->setMaxNumReqInFlight(GetConfig()->GetValue<std::int64_t>(TfBuilderRpcImpl::OptionKeyMaxNumTransfers));
  mRpc->subscribeParameters(*GetConfig());

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

bool TfBuilderDevice::start()
{
  while (!mRpc->start(mTfDataRegionSize)) {
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

void TfBuilderDevice::PreRun()
{
  // update running state
  auto& lStatus = mDiscoveryConfig->status();
  lStatus.mutable_info()->set_process_state(BasicInfo::RUNNING);
  lStatus.mutable_partition()->set_run_number(DataDistLogger::sRunNumber);
  mDiscoveryConfig->write();

  // make directory for the file sink
  mFileSink.makeDirectory();

  // enable monitoring
  DataDistMonitor::enable_datadist(DataDistLogger::sRunNumber, mPartitionId);

  // reset tf counters and start accepting tfs
  // NOTE: TfBuilder accepts TFs in both READY and RUNNING state.
  mFlpInputHandler->reset();
  mRpc->reset_run_counters();
  mRpc->startAcceptingTfs();

  IDDLOG("Entering running state. RunNumber: {}", DataDistLogger::sRunNumberStr);

  mTfFwdTotalDataSize = 0;
  mTfFwdTotalTfCount = 0;

  // In Running state
  mInRunningState = true;
  DDMON("tfbuilder", "running", 1);
}

// Get here when ConditionalRun returns false
void TfBuilderDevice::PostRun()
{
  // stop accepting tfs until the state is clear in-between runs
  mRpc->stopAcceptingTfs();

  // send EOS
  mShouldSendEos = true;

  // Not in Running state
  mInRunningState = false;
  DDMON("tfbuilder", "running", 0);

  // update running state
  auto& lStatus = mDiscoveryConfig->status();
  lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
  mDiscoveryConfig->write();

  // disable monitoring
  DataDistMonitor::disable_datadist();

  // start accepting tfs for the next run
  mRpc->startAcceptingTfs();

  IDDLOG("Exiting running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

bool TfBuilderDevice::ConditionalRun()
{
  if (mShouldExit || mRpc->isTerminateRequested()) {
    return false;
  }
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);

  {
    static unsigned sRateLimit_Monitoring = 0;
    if ((sRateLimit_Monitoring++ % 32) == 0) {
      DDMON("tfbuilder", "running", 1);
    }
  }
  return true;
}

void TfBuilderDevice::TfForwardThread()
{
  using hres_clock = std::chrono::high_resolution_clock;
  auto lRateStartTime = hres_clock::now();

  while (mRunning) {
    std::optional<std::unique_ptr<SubTimeFrame>> lTfOpt = dequeue_for(eTfFwdIn, 100ms);
    if (!mRunning) {
      DDDLOG("TfForwardThread: Not running... ");
      break;
    } else if (!is_running(eTfFwdIn)) {
      DDDLOG("TfForwardThread: Queue closed. Exiting... ");
      break;
    } else if (!mInRunningState) {
      if (lTfOpt) {
        WDDLOG_RL(1000, "Dropping a raw TimeFrame because stop of the run is requested.");
      }

      // send EOS if exiting the running state
      if (dplEnabled() && mShouldSendEos) {
        mTfDplAdapter->sendEosToDpl();
        mShouldSendEos = false;
      }
      continue;
    }

    if (lTfOpt == std::nullopt) {
      DDMON("tfbuilder", "data_output.rate", 0);
      DDMON("tfbuilder", "tf_output.sent_size", mTfFwdTotalDataSize);
      DDMON("tfbuilder", "tf_output.sent_count", mTfFwdTotalTfCount);
      continue;
    }

    auto &lTf = lTfOpt.value();
    const auto lTfId = lTf->id();
    {
      const auto lStfDur = std::chrono::duration<double>(hres_clock::now() - lRateStartTime);
      lRateStartTime = hres_clock::now();
      const auto lRate = (1.0 / lStfDur.count());
      DDMON("tfbuilder", "tf_output.id", lTfId);
      DDMON("tfbuilder", "tf_output.size", lTf->getDataSize());
      DDMON("tfbuilder", "tf_output.rate", lRate);
      DDMON("tfbuilder", "data_output.rate", (lRate * lTf->getDataSize()));

      mTfFwdTotalDataSize += lTf->getDataSize();
      mTfFwdTotalTfCount += 1;
      DDMON("tfbuilder", "tf_output.sent_size", mTfFwdTotalDataSize);
      DDMON("tfbuilder", "tf_output.sent_count", mTfFwdTotalTfCount);
    }

    if (!mStandalone) {
      try {
        IDDLOG_RL(5000, "Forwarding a new TF to DPL. tf_id={} stf_size={:d} unique_equipments={:d} total={:d}",
          lTfId, lTf->getDataSize(), lTf->getEquipmentIdentifiers().size(), mTfFwdTotalTfCount);

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
      }
    }

    // decrement the size used by the TF
    mRpc->recordTfForwarded(lTfId);
  }

  // leaving the output thread, send end of the stream info
  if (dplEnabled() && mShouldSendEos) {
    mTfDplAdapter->sendEosToDpl();
    mShouldSendEos = false;
  }

  DDDLOG("Exiting TF forwarding thread.");
}

} /* namespace o2::DataDistribution */
