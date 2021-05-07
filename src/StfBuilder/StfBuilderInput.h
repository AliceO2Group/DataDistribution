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

#include <SubTimeFrameBuilder.h>
#include <ConcurrentQueue.h>
#include <Utilities.h>

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

  void start();
  void stop();

  void setRunningState(bool pRunning) {
    mLastSeqStfId = 0;
    mAcceptingData = pRunning;
  }

  void DataHandlerThread();
  void StfBuilderThread();
  void StfSequencerThread();

  double StfTimeMean() const { return mStfTimeMean; }
 private:
  /// Main SubTimeBuilder O2 device
  StfBuilderDevice &mDevice;

  /// Thread for the input channel
  bool mRunning = false;
  bool mAcceptingData = false;
  std::thread mInputThread;

  double mStfTimeMean = 1.0;

  /// StfBuilding thread and queues
  std::unique_ptr<ConcurrentFifo<std::vector<FairMQMessagePtr>>> mBuilderInputQueue = nullptr;
  std::unique_ptr<SubTimeFrameReadoutBuilder> mStfBuilder = nullptr;
  std::thread mBuilderThread;

  /// StfSequencer thread
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mSeqStfQueue;
  std::uint64_t mLastSeqStfId = 0;
  std::thread mStfSeqThread;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STFBUILDER_INPUT_H_ */
