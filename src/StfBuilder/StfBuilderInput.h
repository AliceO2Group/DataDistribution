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

#ifndef ALICEO2_STFBUILDER_INPUT_H_
#define ALICEO2_STFBUILDER_INPUT_H_

#include <DataDistMonitoring.h>
#include <SubTimeFrameBuilder.h>
#include <ConcurrentQueue.h>
#include <Utilities.h>
#include <ConfigConsul.h>

#include <Headers/DataHeader.h>

#include <thread>
#include <vector>

namespace o2::DataDistribution
{

class StfBuilderDevice;

class StfInputInterface
{

public:
  static constexpr uint8_t sReadoutInterfaceVersion = 2;

  StfInputInterface() = delete;
  StfInputInterface(StfBuilderDevice &pStfBuilderDev)
    : mDevice(pStfBuilderDev)
  { }
  void start(bool pBuildStf, std::shared_ptr<ConsulStfBuilder> pDiscoveryConfig);
  void stop();

  void setRunningState(bool pRunning) {
    // reset counters when starting the run
    if (pRunning && !mAcceptingData) {
      mStfIdReceiving = 0;
      mStfIdBuilding = 0;
      mLastSeqStfId = 0;
      mMissingStfs = 0;
    }

    mAcceptingData = pRunning;

    DDMON("stfbuilder", "stf_input.missing_stf.total", 0);
  }

  void StfReceiverThread();
  void StfBuilderThread();
  void TopologicalStfBuilderThread(); // threshold scans
  void StfSequencerThread();

 private:
  /// Main SubTimeBuilder O2 device
  StfBuilderDevice &mDevice;

  /// Discovery configuration
  std::shared_ptr<ConsulStfBuilder> mDiscoveryConfig;

  /// Thread for the input channel
  bool mRunning = false;
  bool mAcceptingData = false;
  bool mBuildStf = true; // STF or equipment (threshold scan)
  std::uint32_t mStfIdReceiving = 0;
  std::thread mInputThread;

  /// StfBuilding thread and queues
  std::unique_ptr<ConcurrentFifo<std::vector<fair::mq::MessagePtr>>> mBuilderInputQueue = nullptr;
  std::unique_ptr<SubTimeFrameReadoutBuilder> mStfBuilder = nullptr;
  std::uint32_t mStfIdBuilding = 0;
  std::thread mBuilderThread;

  /// StfSequencer thread
  std::thread mStfSeqThread;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mSeqStfQueue;
  std::uint64_t mLastSeqStfId = 0;
  std::uint64_t mMissingStfs = 0;

};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STFBUILDER_INPUT_H_ */
