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

#include <options/FairMQProgOptions.h>
#include <FairMQLogger.h>

#include <TH1.h>

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
    mDiscoveryConfig(std::make_shared<ConsulTfBuilder>(ProcessType::TfBuilder, Config::getEndpointOption(*GetConfig()))),
    mRpc(std::make_shared<TfBuilderRpcImpl>(mDiscoveryConfig)),
    mFlpInputHandler(*this, mRpc, eTfBuilderOut),
    mFileSink(*this, *this, eTfFileSinkIn, eTfFileSinkOut),
    mTfSizeSamples(),
    mTfFreqSamples()
{
}

TfBuilderDevice::~TfBuilderDevice()
{
}

void TfBuilderDevice::Init()
{
  {
    mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
    mTfBufferSize = GetConfig()->GetValue<std::uint64_t>(OptionKeyTfMemorySize);
    mBuildHistograms = GetConfig()->GetValue<bool>(OptionKeyGui);

    auto &lStatus =  mDiscoveryConfig->status();
    lStatus.mutable_info()->set_type(TfBuilder);
    lStatus.mutable_info()->set_process_id(Config::getIdOption(*GetConfig()));
    lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*GetConfig()));

    mPartitionId = Config::getPartitionOption(*GetConfig());
    lStatus.mutable_partition()->set_partition_id(mPartitionId);

    // File sink
    if (!mFileSink.loadVerifyConfig(*(this->GetConfig())))
      exit(-1);
  }

  {
    auto &lStatus = mDiscoveryConfig->status();

    // finish discovery information: gRPC server
    int lRpcRealPort = 0;
    mRpc->initDiscovery(lStatus.info().ip_address(), lRpcRealPort);
    lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));

    if (! mDiscoveryConfig->write(true)) {
      LOG(ERROR) << "Can not start TfBuilder with id: " << lStatus.info().process_id();
      LOG(ERROR) << "Process with the same id already running?";
      exit(-1);
    }

    mRunning = true;

    // start all gRPC clients
    mRpc->start(mTfBufferSize << 20);

    // start TF forwarding thread
    mTfFwdThread = std::thread(&TfBuilderDevice::TfForwardThread, this);
    // start file sink
    mFileSink.start();

    // Start input handlers
    if (!mFlpInputHandler.start(mDiscoveryConfig)) {
      LOG(ERROR) << "Could not initialize input connections. Exiting.";
      Reset();
      exit(-1);
    }

    mRpc->startAcceptingTfs();

    // start the gui thread
    if (mBuildHistograms) {
      mGui = std::make_unique<RootGui>("TFBuilder", "TF Builder", 1200, 400);
      mGui->Canvas().Divide(3, 1);
      mGuiThread = std::thread(&TfBuilderDevice::GuiThread, this);
    }
  }
}

void TfBuilderDevice::Reset()
{
  mRpc->stopAcceptingTfs();

  mRunning = false;

  // Stop the pipeline
  stopPipeline();

  // stop output handlers
  mFlpInputHandler.stop(mDiscoveryConfig);
  // signal and wait for the output thread
  mFileSink.stop();
  // join on fwd thread
  if (mTfFwdThread.joinable()) {
    mTfFwdThread.join();
  }

  //wait for the gui thread
  if (mBuildHistograms && mGuiThread.joinable()) {
    mGuiThread.join();
  }

  // stop the RPCs
  mRpc->stop();

  mDiscoveryConfig.reset();

  LOG(DEBUG) << "Reset() done... ";
}

bool TfBuilderDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(1s);
  return true;
}

void TfBuilderDevice::TfForwardThread()
{
  // wait for the device to go into RUNNING state

  while (mRunning) {
    auto lFreqStartTime = std::chrono::high_resolution_clock::now();

    std::unique_ptr<SubTimeFrame> lTf = dequeue(eTfFwdIn);
    if (!lTf) {
      LOG(WARNING) << "ConditionalRun(): Exiting... ";
      break;
    }

    // record frequency and size of TFs
    if (mBuildHistograms) {
      mTfFreqSamples.Fill(
        1.0 / std::chrono::duration<double>(
                std::chrono::high_resolution_clock::now() - lFreqStartTime)
                .count());

      lFreqStartTime = std::chrono::high_resolution_clock::now();

      // size histogram
      mTfSizeSamples.Fill(lTf->getDataSize());
    }

    // TODO: Do something with the TF
    {
      // is there a ratelimited LOG?
      static unsigned long floodgate = 0;
      if (++floodgate % 44 == 1)
        LOG(DEBUG) << "TF[" << lTf->header().mId << "] size: " << lTf->getDataSize();
    }

    // decrement the size used by the TF
    // TODO: move this close to the output channel send
    mRpc->recordTfForwarded(*lTf);
  }

  LOG(INFO) << "Exiting TF forwarding thread... ";
}

void TfBuilderDevice::GuiThread()
{
  std::unique_ptr<TH1F> lTfSizeHist = std::make_unique<TH1F>("TfSizeH", "Size of TF", 100, 0.0, float(1UL << 30));
  lTfSizeHist->GetXaxis()->SetTitle("Size [B]");

  std::unique_ptr<TH1F> lTfFreqHist = std::make_unique<TH1F>("TfFreq", "TimeFrame frequency", 200, 0.0, 100.0);
  lTfFreqHist->GetXaxis()->SetTitle("Frequency [Hz]");

  std::unique_ptr<TH1S> lStfPipelinedCntHist = std::make_unique<TH1S>("StfQueuedH", "Queued STFs", 150, -0.5, 150.0 - 0.5);
  lStfPipelinedCntHist->GetXaxis()->SetTitle("Number of queued Stf");

  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {
    LOG(INFO) << "Updating histograms...";

    mGui->Canvas().cd(1);
    mGui->DrawHist(lTfSizeHist.get(), mTfSizeSamples);

    mGui->Canvas().cd(2);
    mGui->DrawHist(lTfFreqHist.get(), mTfFreqSamples);

    mGui->Canvas().cd(3);
    mGui->DrawHist(lStfPipelinedCntHist.get(), this->getPipelinedSizeSamples());

    mGui->Canvas().Modified();
    mGui->Canvas().Update();

    std::this_thread::sleep_for(5s);
  }

  LOG(INFO) << "Exiting GUI thread...";
}
}
} /* namespace o2::DataDistribution */
