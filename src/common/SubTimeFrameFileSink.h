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

#ifndef ALICEO2_SUBTIMEFRAME_FILE_SINK_H_
#define ALICEO2_SUBTIMEFRAME_FILE_SINK_H_

#include "SubTimeFrameDataModel.h"
#include "SubTimeFrameFileWriter.h"
#include "ConcurrentQueue.h"
#include "DataDistLogger.h"

#include <Headers/DataHeader.h>

#include <fairmq/ProgOptionsFwd.h>

#include <boost/program_options/options_description.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>

namespace o2::DataDistribution
{

namespace bpo = boost::program_options;

class DataDistDevice;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileSink
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameFileSink
{
  using stf_pipeline = IFifoPipeline<std::unique_ptr<SubTimeFrame>>;

 public:
  static constexpr const char* OptionKeyStfSinkEnable = "data-sink-enable";
  static constexpr const char* OptionKeyStfSinkDir = "data-sink-dir";
  static constexpr const char* OptionKeyStfSinkFileName = "data-sink-file-name";
  static constexpr const char* OptionKeyStfSinkStfsPerFile = "data-sink-max-stfs-per-file";
  static constexpr const char* OptionKeyStfSinkStfPercent = "data-sink-stf-percentage";
  static constexpr const char* OptionKeyStfSinkFileSize = "data-sink-max-file-size";
  static constexpr const char* OptionKeyStfSinkSidecar = "data-sink-sidecar";
  static constexpr const char* OptionKeyStfSinkEpn2EosMetaDir = "data-sink-epn2eos-meta-dir";
  static bpo::options_description getProgramOptions();

  SubTimeFrameFileSink() = delete;

  SubTimeFrameFileSink(DataDistDevice& pDevice, stf_pipeline& pPipeline, unsigned pPipelineStageIn, unsigned pPipelineStageOut)
    : mDeviceI(pDevice),
      mPipelineI(pPipeline),
      mPipelineStageIn(pPipelineStageIn),
      mPipelineStageOut(pPipelineStageOut)
  {
    DDDLOG("(Sub)TimeFrame Sink created.");
  }

  ~SubTimeFrameFileSink()
  {
    if (mSinkThread.joinable()) {
      mSinkThread.join();
    }
    DDDLOG("(Sub)TimeFrame Sink terminated.");
  }

  bool loadVerifyConfig(const fair::mq::ProgOptions& pFMQProgOpt);
  bool makeDirectory();

  bool enabled() const { return mEnabled; }

  void start();
  void stop();
  void flush() { mCloseWriter = true; };

  void DataHandlerThread(const unsigned pIdx);

  std::string newStfFileName(const std::uint64_t pStfId) const;

 private:
  const DataDistDevice& mDeviceI;
  stf_pipeline& mPipelineI;

  bool mCloseWriter;
  std::unique_ptr<SubTimeFrameFileWriter> mStfWriter = nullptr;

  /// Configuration
  bool mEnabled = false;
  bool mRunning = false;
  bool mReady = false;
  std::string mRootDir;
  std::string mCurrentDir;
  std::string mFileNamePattern;
  std::uint64_t mStfsPerFile;
  double mPercentageToSave = 100.0;
  std::uint64_t mFileSize;
  bool mSidecar = false;
  std::string mEosMetaDir;
  std::optional<EosMetadata> mEosMetadataOpt = std::nullopt;
  std::string mHostname;

  /// Thread for file writing
  std::thread mSinkThread;
  unsigned mPipelineStageIn;
  unsigned mPipelineStageOut;

  /// variables
  unsigned mCurrentFileIdx = 0;
};

} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_SINK_H_ */
