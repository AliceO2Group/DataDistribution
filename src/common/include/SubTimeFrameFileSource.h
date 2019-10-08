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

#ifndef ALICEO2_SUBTIMEFRAME_FILE_SOURCE_H_
#define ALICEO2_SUBTIMEFRAME_FILE_SOURCE_H_

#include "ConcurrentQueue.h"
#include "SubTimeFrameBuilder.h"

#include <boost/program_options/options_description.hpp>

#include <fstream>
#include <vector>

namespace o2
{
namespace DataDistribution
{

namespace bpo = boost::program_options;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileSource
////////////////////////////////////////////////////////////////////////////////
class SubTimeFrameFileReader;
class SubTimeFrame;

class SubTimeFrameFileSource
{
  using stf_pipeline = IFifoPipeline<std::unique_ptr<SubTimeFrame>>;

 public:
  static constexpr const char* OptionKeyStfSourceEnable = "data-source-enable";
  static constexpr const char* OptionKeyStfSourceDir = "data-source-dir";
  static constexpr const char* OptionKeyStfLoadRate = "data-source-rate";
  static constexpr const char* OptionKeyStfSourceRepeat = "data-source-repeat";

  static bpo::options_description getProgramOptions();

  SubTimeFrameFileSource() = delete;

  SubTimeFrameFileSource(stf_pipeline& pPipeline, unsigned pPipelineStageOut)
    : mPipelineI(pPipeline),
      mPipelineStageOut(pPipelineStageOut)
  {
  }

  ~SubTimeFrameFileSource()
  {
    if (mSourceThread.joinable()) {
      mSourceThread.join();
    }
    LOG(INFO) << "(Sub)TimeFrame Source terminated...";
  }

  bool loadVerifyConfig(const FairMQProgOptions& pFMQProgOpt);
  std::vector<std::string> getDataFileList() const;

  bool enabled() const { return mEnabled; }

  void start(FairMQChannel& pDstChan, const bool pDplEnabled);
  void stop();

  void DataHandlerThread();
  void DataInjectThread();

 private:
  stf_pipeline& mPipelineI;

  unsigned mPipelineStageOut;
  /// Destination channel to send the Stfs to (allocation optimization)
  FairMQChannel *mDstChan = nullptr;
  std::unique_ptr<SubTimeFrameFileBuilder> mFileBuilder;

  /// Configuration
  bool mEnabled = false;
  bool mDplEnabled = false;
  std::string mDir;
  bool mRepeat = false;
  std::uint64_t mLoadRate = 44;

  /// Thread for file writing
  std::atomic_bool mRunning = false;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mReadStfQueue;
  std::thread mSourceThread;
  std::thread mInjectThread;

};
}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_SOURCE_H_ */
