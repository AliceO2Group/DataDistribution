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

#include "DataDistLogger.h"

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
  static constexpr const char* OptionKeyStfLoadPreRead = "data-source-preread";
  static constexpr const char* OptionKeyStfSourceRepeat = "data-source-repeat";
  static constexpr const char* OptionKeyStfSourceRegionSize = "data-source-regionsize";
  static constexpr const char* OptionKeyStfHeadersRegionSize = "data-source-headersize";


  static bpo::options_description getProgramOptions();

  SubTimeFrameFileSource() = delete;

  SubTimeFrameFileSource(stf_pipeline& pPipeline, unsigned pPipelineStageOut)
    : mPipelineI(pPipeline),
      mPipelineStageOut(pPipelineStageOut)
  {
    DDDLOG("(Sub)TimeFrame Source started...");
  }

  ~SubTimeFrameFileSource()
  {
    stop();
    DDDLOG("(Sub)TimeFrame Source terminated...");
  }

  bool loadVerifyConfig(const FairMQProgOptions& pFMQProgOpt);
  std::vector<std::string> getDataFileList() const;

  bool enabled() const { return mEnabled; }

  void start(MemoryResources &pMemRes, const bool pDplEnabled);
  void pause() { mPaused = true; }
  void resume() { mPaused = false; }
  void stop();

  void DataHandlerThread();
  void DataInjectThread();

 private:
  stf_pipeline& mPipelineI;

  unsigned mPipelineStageOut;
  std::unique_ptr<SubTimeFrameFileBuilder> mFileBuilder;

  /// Configuration
  bool mEnabled = false;
  bool mDplEnabled = false;
  std::string mDir;
  std::vector<std::string> mFilesVector;
  bool mRepeat = false;
  double mLoadRate = 1.f;
  std::uint32_t mPreReadStfs = 1;
  std::size_t mRegionSizeMB = 1024; /* 1GB in MiB */
  std::size_t mHdrRegionSizeMB = 256;

  /// Thread for file writing
  std::atomic_bool mRunning = false;
  std::atomic_bool mPaused = false;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mReadStfQueue;
  std::thread mSourceThread;
  std::thread mInjectThread;
};

}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_SOURCE_H_ */
