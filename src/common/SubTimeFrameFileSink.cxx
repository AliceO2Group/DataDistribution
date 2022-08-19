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

#include "SubTimeFrameFileSink.h"
#include "SubTimeFrameFile.h"
#include "FilePathUtils.h"
#include "FmqUtilities.h"
#include "DataDistLogger.h"

#include <fairmq/ProgOptions.h>

#include <DataFormatsParameters/ECSDataAdapters.h>

#include <boost/asio/ip/host_name.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/filesystem.hpp>

#include <chrono>
#include <ctime>
#include <iostream>
#include <iomanip>
#include <random>

#ifdef __linux__
  #include <unistd.h>
  #include <string.h>
#endif

namespace o2::DataDistribution
{

namespace bpo = boost::program_options;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileSink
////////////////////////////////////////////////////////////////////////////////

void SubTimeFrameFileSink::start()
{
  if (enabled()) {
    const auto lHostname = boost::asio::ip::host_name();
    mHostname = lHostname.substr(0, lHostname.find('.'));
    mRunning = true;
    mSinkThread = create_thread_member("stf_sink", &SubTimeFrameFileSink::DataHandlerThread, this, 0);
  }
  DDDLOG("SubTimeFrameFileSink started");
}

void SubTimeFrameFileSink::stop()
{
  mRunning = false;

  if (mSinkThread.joinable()) {
    mSinkThread.join();
  }
}

bpo::options_description SubTimeFrameFileSink::getProgramOptions()
{
  bpo::options_description lSinkDesc("(Sub)TimeFrame file sink options", 120);

  lSinkDesc.add_options()(
    OptionKeyStfSinkEnable,
    bpo::bool_switch()->default_value(false),
    "Enable writing of (Sub)TimeFrames to file.")(
    OptionKeyStfSinkDir,
    bpo::value<std::string>()->default_value(""),
    "Specifies a destination directory where (Sub)TimeFrames are to be written. "
    "Note: A new directory will be created here for all output files.")(
    OptionKeyStfSinkFileName,
    bpo::value<std::string>()->default_value("o2_rawtf_run%r_tf%i_%h.tf"),
    "Specifies file name pattern: %n - file index, %r - run number, %i - (S)TF id, %D - date, %T - time, %h - hostname.")(
    OptionKeyStfSinkStfsPerFile,
    bpo::value<std::uint64_t>()->default_value(1),
    "Specifies number of (Sub)TimeFrames per file. Default: 1")(
    OptionKeyStfSinkStfPercent,
    bpo::value<double>()->default_value(100.0),
    "Specifies probabilistic acceptance percentage for saving of each (Sub)TimeFrames, between 0.0 and 100. Default: 100.0")(
    OptionKeyStfSinkFileSize,
    bpo::value<std::uint64_t>()->default_value(std::uint64_t(4) << 10), /* 4GiB */
    "Specifies target size for (Sub)TimeFrame files in MiB.")(
    OptionKeyStfSinkSidecar,
    bpo::bool_switch()->default_value(false),
    "Write a sidecar file for each (Sub)TimeFrame file containing information about data blocks "
    "written in the data file. "
    "Note: Useful for debugging. "
    "Warning: sidecar file format is not stable.")(
    OptionKeyStfSinkEpn2EosMetaDir,
    bpo::value<std::string>()->default_value(""),
    "Specify the directory where EPN2EOS metadata will be created. No metadata is created if empty (default).");

  return lSinkDesc;
}

bool SubTimeFrameFileSink::loadVerifyConfig(const fair::mq::ProgOptions& pFMQProgOpt)
{
  mEnabled = pFMQProgOpt.GetValue<bool>(OptionKeyStfSinkEnable);

  IDDLOG("(Sub)TimeFrame file sink is {}", (mEnabled ? "enabled." : "disabled."));

  if (!mEnabled) {
    return true;
  }

  // set enabled to false until all tests pass
  mEnabled = false;

  mRootDir = pFMQProgOpt.GetValue<std::string>(OptionKeyStfSinkDir);
  if (mRootDir.length() == 0) {
    EDDLOG("(Sub)TimeFrame file sink directory must be specified");
    return false;
  }

  mFileNamePattern = pFMQProgOpt.GetValue<std::string>(OptionKeyStfSinkFileName);
  mStfsPerFile = pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfSinkStfsPerFile);
  mFileSize = std::max(std::uint64_t(1), pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfSinkFileSize));
  mPercentageToSave = std::clamp(pFMQProgOpt.GetValue<double>(OptionKeyStfSinkStfPercent), 0.0, 100.0);
  mFileSize <<= 20; /* in MiB */
  mSidecar = pFMQProgOpt.GetValue<bool>(OptionKeyStfSinkSidecar);
  mEosMetaDir = pFMQProgOpt.GetValue<std::string>(OptionKeyStfSinkEpn2EosMetaDir);

  // Check if save percentage is zero
  if (mPercentageToSave < std::numeric_limits<double>::epsilon()) {
    IDDLOG("(Sub)TimeFrame file sink disabled due to low save percentage. {}", mPercentageToSave);
    mEnabled = false;
    return true;
  }

  // make sure directory exists and it is writable
  namespace bfs = boost::filesystem;
  bfs::path lDirPath(mRootDir);
  if (!bfs::is_directory(lDirPath)) {
    EDDLOG("(Sub)TimeFrame file sink directory does not exist. dir={}", mRootDir);
    return false;
  }

  // check if metadata dir exists and it is writable
  if (!mEosMetaDir.empty()) {
    bfs::path lEosMetaPath(mEosMetaDir);
    if (!bfs::is_directory(lEosMetaPath)) {
      EDDLOG("(Sub)TimeFrame file EPN2EOS metadata directory does not exist. epn2eos_meta_dir={}", mEosMetaDir);
      return false;
    }

#ifdef __linux__
    // check write permissions
    char *lEosMetaDirCopy = strdup(mEosMetaDir.c_str());
    if (0 != access(lEosMetaDirCopy, W_OK)) {
      EDDLOG("(Sub)TimeFrame file EPN2EOS metadata directory is not writeable. epn2eos_meta_dir={}", mEosMetaDir);
    }
    free(lEosMetaDirCopy);
#endif

    // Fetching required and optional configuration from ECS
    // "lhc_period" if empty, replace by the current month, eg, JAN
    // "run_type" : use a method from o2 to convert AliECS properties to the file type, it should match the ctf
    // "detectors" list of detectors, optional

    EosMetadata lEosMetadata;
    lEosMetadata.mEosMetaDir = mEosMetaDir;
    lEosMetadata.mLhcPeriod = pFMQProgOpt.GetPropertyAsString("lhc_period", "");
    lEosMetadata.mDetectorList = pFMQProgOpt.GetPropertyAsString("detectors", "");
    lEosMetadata.mFileType = o2::parameters::GRPECS::getRawDataPersistencyMode(
      pFMQProgOpt.GetPropertyAsString("run_type", "NONE"),
      boost::iequals(pFMQProgOpt.GetPropertyAsString("force_run_as_raw", "false"), "true")
    );

    if (lEosMetadata.mLhcPeriod.empty()) {
      lEosMetadata.mLhcPeriod = getDefaultLhcPeriod();
      WDDLOG("(Sub)TimeFrame file EPN2EOS metadata: lhc_period not set by ECS. Using default lhc_period={}", lEosMetadata.mLhcPeriod);
    }

    if (lEosMetadata.mFileType.empty()) {
      lEosMetadata.mFileType = "raw";
      WDDLOG("(Sub)TimeFrame file EPN2EOS metadata: run_type not set by ECS. Using default run_type={}", lEosMetadata.mFileType);
    }

    mEosMetadataOpt = std::move(lEosMetadata);
  }

  mEnabled = true;

  // print options
  IDDLOG("(Sub)TimeFrame Sink :: enabled          = {}", (mEnabled ? "yes" : "no"));
  IDDLOG("(Sub)TimeFrame Sink :: root dir         = {}", mRootDir);
  IDDLOG("(Sub)TimeFrame Sink :: file pattern     = {}", mFileNamePattern);
  IDDLOG("(Sub)TimeFrame Sink :: stfs per file    = {}", (mStfsPerFile > 0 ? std::to_string(mStfsPerFile) : "unlimited" ));
  IDDLOG("(Sub)TimeFrame Sink :: stfs percentage  = {:.4}", (mPercentageToSave));
  IDDLOG("(Sub)TimeFrame Sink :: max file size    = {}", mFileSize);
  IDDLOG("(Sub)TimeFrame Sink :: sidecar files    = {}", (mSidecar ? "yes" : "no"));
  IDDLOG("(Sub)TimeFrame Sink :: epn2eos meta dir = {}", mEosMetaDir);
  if (!mEosMetaDir.empty()) {
    IDDLOG("(Sub)TimeFrame Sink :: epn2eos meta :: lhc_period = {}", mEosMetadataOpt.value().mLhcPeriod);
    IDDLOG("(Sub)TimeFrame Sink :: epn2eos meta :: run_type   = {}", mEosMetadataOpt.value().mFileType);
    IDDLOG("(Sub)TimeFrame Sink :: epn2eos meta :: detectors  = {}", mEosMetadataOpt.value().mDetectorList);
  }

  return mEnabled;
}

bool SubTimeFrameFileSink::makeDirectory()
{
  namespace bfs = boost::filesystem;

  if (!enabled()) {
    return true;
  }

  try {
    fmt::memory_buffer lDir;
    fmt::format_to(fmt::appender(lDir), "run0{}_{}", DataDistLogger::sRunNumberStr, FilePathUtils::getDataDirName(mRootDir));
    mCurrentDir = (bfs::path(mRootDir) / bfs::path(lDir.begin(), lDir.end())).string();

    // make the run directory
    if (!bfs::create_directory(mCurrentDir)) {
      EDDLOG("Directory for (Sub)TimeFrame file sink cannot be created. Disabling file sink. path={}", mCurrentDir);
      mEnabled = false;
      return false;
    }
  } catch (...) {
    EDDLOG("(Sub)TimeFrame Sink :: write directory creation failed. File sink will be disables. dir={}", mCurrentDir);
    mReady = false;
    return false;
  }

  IDDLOG("(Sub)TimeFrame Sink :: write dir={:s}", mCurrentDir);

  // EPN2EOS meta: set run number
  if (mEosMetadataOpt) {
    mEosMetadataOpt.value().mRunNumber = DataDistLogger::sRunNumberStr;
  }

  mReady = true;
  return true;
}

std::string SubTimeFrameFileSink::newStfFileName(const std::uint64_t pStfId) const
{
  time_t lNow;
  time(&lNow);
  char lTimeBuf[256];

  std::string lFileName = mFileNamePattern;

  // file index
  std::stringstream lIdxString;
  lIdxString << std::dec << std::setw(8) << std::setfill('0') << mCurrentFileIdx;
  boost::replace_all(lFileName, "%n", lIdxString.str());

  // run id
  std::stringstream lRunString;
  const auto lWidth = std::max(std::size_t(8), DataDistLogger::sRunNumberStr.size());
  lRunString << std::dec << std::setw(lWidth) << std::setfill('0') << DataDistLogger::sRunNumber;
  boost::replace_all(lFileName, "%r", lRunString.str());

  // stf id
  std::stringstream lStfIdString;
  lStfIdString << std::dec << std::setw(8) << std::setfill('0') << pStfId;
  boost::replace_all(lFileName, "%i", lStfIdString.str());

  // date
  strftime(lTimeBuf, sizeof(lTimeBuf), "%F", localtime(&lNow));
  boost::replace_all(lFileName, "%D", lTimeBuf);

  // time
  strftime(lTimeBuf, sizeof(lTimeBuf), "%H_%M_%S", localtime(&lNow));
  boost::replace_all(lFileName, "%T", lTimeBuf);

  // hostname
  boost::replace_all(lFileName, "%h", mHostname);

  return lFileName;
}

/// File writing thread
void SubTimeFrameFileSink::DataHandlerThread(const unsigned pIdx)
{
  using namespace std::chrono_literals;
  std::hash<std::thread::id> lSeedHasher;
  std::default_random_engine lGen(lSeedHasher(std::this_thread::get_id()) * getpid());
  std::uniform_real_distribution<double> lUniformDist(0, 100.0);
  std::uint64_t lAcceptedStfs = 0;
  std::uint64_t lTotalStfs = 0;

  std::uint64_t lCurrentFileSize = 0;
  std::uint64_t lCurrentFileStfs = 0;

  std::string lCurrentFileName;

  while (mRunning) {
    // Get the next STF
    auto lStfOpt = mPipelineI.dequeue_for(mPipelineStageIn, 500000us);
    if (!lStfOpt) {

      // check if should flush the file
      if (mCloseWriter) {
        mCloseWriter = false;
        lCurrentFileStfs = 0;
        lCurrentFileSize = 0;
        mStfWriter.reset();
      }
      continue;
    }

    std::unique_ptr<SubTimeFrame> lStf = std::move(lStfOpt.value());

    lTotalStfs += 1;

    if (mEnabled && !mReady) {
      EDDLOG_RL(5000, "SubTimeFrameFileSink is not ready! Missed the RUN transition?");
    }

    // apply rejection rules
    bool lStfAccepted = (lUniformDist(lGen) <= mPercentageToSave) ? true : false;

    if (mEnabled && mReady && lStfAccepted) {
      do {
        lAcceptedStfs += 1;
        // make sure Stf is updated before writing
        lStf->updateStf();

        // check if we need a writer
        if (!mStfWriter) {
          const auto lStfId = lStf->id();
          lCurrentFileName = newStfFileName(lStfId);
          namespace bfs = boost::filesystem;

          try {
            mStfWriter = std::make_unique<SubTimeFrameFileWriter>(
              bfs::path(mCurrentDir) / bfs::path(lCurrentFileName), mSidecar, mEosMetadataOpt);
          } catch (...) {
            mStfWriter.reset();
            break;
          }
            mCurrentFileIdx++;
        }

        // write
        if (mStfWriter->write(*lStf)) {
          lCurrentFileStfs++;
          lCurrentFileSize = mStfWriter->size();
        } else {
          mStfWriter->close();
          mStfWriter->remove();
          mStfWriter.reset();
          break;
        }

        // check if we should rotate the file
        if (((mStfsPerFile > 0) && (lCurrentFileStfs >= mStfsPerFile)) || (lCurrentFileSize >= mFileSize)) {
          lCurrentFileStfs = 0;
          lCurrentFileSize = 0;
          mStfWriter.reset();
        }
      } while(0);

      if (!mEnabled) {
        EDDLOG("(Sub)TimeFrame file sink: error while writing to file {}", lCurrentFileName);
        EDDLOG("(Sub)TimeFrame file sink: disabling file sink");
      }
    }

    if (! mPipelineI.queue(mPipelineStageOut, std::move(lStf)) ) {
      // the pipeline is stopped: exiting
      break;
    }
  }

  // flush the file if still open
  if (mStfWriter) {
    mStfWriter.reset();
  }

  IDDLOG("(Sub)TimeFrame file sink: saved={} total={}", lAcceptedStfs, lTotalStfs);
  DDDLOG("Exiting file sink thread [{}]", pIdx);
}

} /* o2::DataDistribution */
