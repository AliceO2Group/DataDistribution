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

#include <ConcurrentQueue.h>
#include <Utilities.h>

#include <Headers/DataHeader.h>

#include <thread>
#include <vector>

namespace o2
{
namespace DataDistribution
{

class StfBuilderDevice;

class StfInputInterface
{
 public:
  StfInputInterface() = delete;
  StfInputInterface(StfBuilderDevice& pStfBuilderDev)
    : mDevice(pStfBuilderDev),
      mStfFreqSamples(),
      mStfNumFilteredMessages()
  {
  }

  void start(const o2::header::DataOrigin &);
  void stop();

  void DataHandlerThread(const unsigned pInputChannelIdx);

  const RunningSamples<float>& StfFreqSamples() const { return mStfFreqSamples; }

  void setRdh4FilterTrigger(bool pVal) { mRdh4FilterTrigger = pVal; }

 private:
  /// Main SubTimeBuilder O2 device
  StfBuilderDevice& mDevice;

  /// Thread for the input channel
  std::atomic_bool mRunning = false;
  std::thread mInputThread;

  RunningSamples<float> mStfFreqSamples;

  /// Readout flags
  bool mRdh4FilterTrigger = false;  // filter out empty HBFs in triggered mode with RDHv4

  bool mFilterTriggerEmpty = false; // Filter out empty CRU blocks in trigger mode
  RunningSamples<float> mStfNumFilteredMessages;

  o2::header::DataOrigin mDataOrigin;
};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STFBUILDER_INPUT_H_ */
