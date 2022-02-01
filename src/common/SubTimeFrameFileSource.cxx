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

#include "SubTimeFrameFileSource.h"
#include "SubTimeFrameFileReader.h"
#include "FilePathUtils.h"
#include "DataDistLogger.h"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/filesystem.hpp>
#include <boost/process.hpp>

#include <chrono>
#include <ctime>
#include <iostream>
#include <iomanip>

namespace o2
{
namespace DataDistribution
{

namespace bpo = boost::program_options;
namespace bfs = boost::filesystem;
namespace bp = boost::process;

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileSource
////////////////////////////////////////////////////////////////////////////////

std::mutex SubTimeFrameFileSource::StfFileMeta::sLiveFilesLock;
std::map<std::size_t, std::shared_ptr<SubTimeFrameFileSource::StfFileMeta>> SubTimeFrameFileSource::StfFileMeta::mLiveFiles;


void SubTimeFrameFileSource::start(MemoryResources &pMemRes)
{
  if (enabled()) {

    mFileBuilder = std::make_unique<SubTimeFrameFileBuilder>(
      pMemRes,
      mRegionSizeMB << 20, mTfDataRegionId,
      mHdrRegionSizeMB << 20, mTfHdrRegionId
    );

    mRunning = true;

    mFetchThread = create_thread_member("stf_file_fetch", &SubTimeFrameFileSource::DataFetcherThread, this);
    mSourceThread = create_thread_member("stf_file_read", &SubTimeFrameFileSource::DataHandlerThread, this);
    mInjectThread = create_thread_member("stf_file_inject", &SubTimeFrameFileSource::DataInjectThread, this);
  }
}

void SubTimeFrameFileSource::stop()
{
  mRunning = false;

  if (mFileBuilder) {
    mFileBuilder->stop();
  }

  mInputFileQueue.stop();

  mReadStfQueue.stop();
  mReadStfQueue.flush();

  if (mFetchThread.joinable()) {
    mFetchThread.join();
  }

  if (mSourceThread.joinable()) {
    mSourceThread.join();
  }

  if (mInjectThread.joinable()) {
    mInjectThread.join();
  }

  // make sure we delete the cache
  if (!mLocalFiles && boost::contains(mCopyDstPath.native(), "dd-tmp-tfs")) {
    try {
      bfs::remove_all(mCopyDstPath);
    } catch (...) {
      WDDLOG("(Sub)TimeFrame file source: could not remove tf file cache. directory={}", mCopyDstPath.native());
    }
  }
}

bpo::options_description SubTimeFrameFileSource::getProgramOptions()
{
  bpo::options_description lSinkDesc("(Sub)TimeFrame file source options", 120);

  lSinkDesc.add_options()(
    OptionKeyStfSourceEnable,
    bpo::bool_switch()->default_value(false),
    "Enable reading of (Sub)TimeFrames from files.")(
    OptionKeyStfSourceDir,
    bpo::value<std::string>()->default_value(""),
    "Specifies the source directory where (Sub)TimeFrame files are located. "
    "Note: Only (Sub)TimeFrame data files are allowed in this directory.")(
    OptionKeyStfLoadRate,
    bpo::value<double>()->default_value(1.0),
    "Rate of injecting new (Sub)TimeFrames (approximate). -1 to inject as fast as possible.")(
    OptionKeyStfLoadPreRead,
    bpo::value<std::uint32_t>()->default_value(1),
    "Number of pre-read (Sub)TimeFrames prepared for sending. Must be greater or equal to 1.")(
    OptionKeyStfSourceRepeat,
    bpo::bool_switch()->default_value(false),
    "If enabled, repeatedly inject (Sub)TimeFrames into the chain.")(
    OptionKeyStfSourceRegionSize,
    bpo::value<std::uint64_t>()->default_value(1024),
    "Size of the memory region for (Sub)TimeFrames data in MiB. "
    "Note: make sure the region can fit several (Sub)TimeFrames to avoid deadlocks.")(
    OptionKeyStfSourceRegionId,
    bpo::value<std::uint16_t>()->default_value(std::uint16_t(~0)),
    "Optional shm id for reusing existing TimeFrame regions. (default will create a new region)")(
    OptionKeyStfHeadersRegionSize,
    bpo::value<std::uint64_t>()->default_value(256),
    "Size of the memory region for (Sub)TimeFrames O2 headers in MiB. "
    "Note: make sure the region can fit several (Sub)TimeFrames to avoid deadlocks.")(
    OptionKeyStfHeadersRegionId,
    bpo::value<std::uint16_t>()->default_value(std::uint16_t(~0)),
    "Optional shm id for reusing existing TimeFrame header region. (default will create a new region)")(
    OptionKeyStfFileList,
    bpo::value<std::string>()->default_value(""),
    "File name which contains the list of files at remote location, e.g. a list of files on EOS, or a remote server. "
    "Note: the copy command must be provided.")(
    OptionKeyStfCopyCmd,
    bpo::value<std::string>()->default_value(""),
    "Copy command to be used to fetch remote files. NOTE: Placeholders for source and destination file name "
    "(?src and ?dst) must be specified. E.g. \"scp user@my-server:?src ?dst\". Source placeholder will be "
    "substituted with files provided in the file-list option.");

  return lSinkDesc;
}

std::vector<std::string> SubTimeFrameFileSource::getDataFileList() const
{
  // Load the sorted list of StfFiles
  auto lFilesVector = FilePathUtils::getAllFiles(mDir);
  // Remove side-car files
  auto lRemIt = std::remove_if(lFilesVector.begin(), lFilesVector.end(),
    [](const std::string &lElem) {

      const auto lFileName = bfs::path(lElem).filename().native();

      bool lToRemove = !boost::ends_with(lFileName, ".tf") || boost::starts_with(lFileName, ".");
      DDDLOG("Checking if should remove file: {} ? {}", lFileName, (lToRemove ? "yes" : "no"));
      return lToRemove;
    }
  );

  lFilesVector.erase(lRemIt, lFilesVector.end());

  return lFilesVector;
}

bool SubTimeFrameFileSource::loadVerifyConfig(const FairMQProgOptions& pFMQProgOpt)
{
  mEnabled = pFMQProgOpt.GetValue<bool>(OptionKeyStfSourceEnable);

  IDDLOG("(Sub)TimeFrame file source is {}", (mEnabled ? "enabled." : "disabled."));

  if (!mEnabled) {
    return true;
  }

  mDir = pFMQProgOpt.GetValue<std::string>(OptionKeyStfSourceDir);
  if (!mDir.empty()) {
    // make sure directory exists and it is readable
    bfs::path lDirPath(mDir);
    if (!bfs::is_directory(lDirPath)) {
      EDDLOG("(Sub)TimeFrame file source directory does not exist.");
      return false;
    }
  }

  mRepeat = pFMQProgOpt.GetValue<bool>(OptionKeyStfSourceRepeat);
  mLoadRate = pFMQProgOpt.GetValue<double>(OptionKeyStfLoadRate);
  mPreReadStfs = pFMQProgOpt.GetValue<std::uint32_t>(OptionKeyStfLoadPreRead);
  mRegionSizeMB = pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfSourceRegionSize);
  mTfDataRegionId = pFMQProgOpt.GetValue<std::uint16_t>(OptionKeyStfSourceRegionId);
  if (mTfDataRegionId.value() == std::uint16_t(~0)) {
    mTfDataRegionId = std::nullopt;
  }

  mHdrRegionSizeMB = pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfHeadersRegionSize);
  mTfHdrRegionId = pFMQProgOpt.GetValue<std::uint16_t>(OptionKeyStfHeadersRegionId);
  if (mTfHdrRegionId.value() == std::uint16_t(~0)) {
    mTfHdrRegionId = std::nullopt;
  }

  mCopyFileList = pFMQProgOpt.GetValue<std::string>(OptionKeyStfFileList);
  mCopyCmd = pFMQProgOpt.GetValue<std::string>(OptionKeyStfCopyCmd);

  // initialize file fetcher
  if (!mCopyFileList.empty() && !mCopyCmd.empty()) {

    if (!mDir.empty()) {
      EDDLOG("(Sub)TimeFrame file source: specifying both the directory, and the file copy options is not supported.");
      return false;
    }
    mLocalFiles = false;

    try {
      // check copy command
      if (!boost::contains(mCopyCmd, "?src") || !boost::contains(mCopyCmd, "?dst")) {
        EDDLOG("(Sub)TimeFrame file source: copy command does not containt ?src and ?dst placeholders.");
        return false;
      }

      // check copy file list
      bfs::path lFileList(mCopyFileList);
      if (!bfs::is_regular_file(lFileList) || bfs::is_empty(lFileList)) {
        EDDLOG("(Sub)TimeFrame file source: File list does not exist or empty");
        return false;
      }

      // load the file list
      std::ifstream lCopyFile(mCopyFileList);
      if (!lCopyFile) {
        EDDLOG("(Sub)TimeFrame file source: cannot open {}.", mCopyFileList);
        return false;
      }

      mFilesVector.clear();
      std::string lLine;
      while (std::getline(lCopyFile, lLine)) {
        boost::algorithm::trim(lLine);

        if (lLine.empty()) {
          continue;
        }
        mFilesVector.push_back(std::move(lLine));
      }

      if (mFilesVector.empty()) {
        EDDLOG("(Sub)TimeFrame file source: no files found in {}.", mCopyFileList);
        return false;
      }

      const auto lCopyDstPath = bfs::temp_directory_path() / "dd-tmp-tfs" / bfs::unique_path("%%%%");
      bfs::create_directories(lCopyDstPath);
      mCopyDstPath = lCopyDstPath.native();
      // location of log file
      mCopyCmdLogFile = (lCopyDstPath / "copy-cmd.log").native();

    } catch (...) {
      EDDLOG("(Sub)TimeFrame file source: setting up file copy failed. Check the provided options.");
      return false;
    }
  } else if (!mDir.empty()) {

    // Local directory files
    mLocalFiles = true;
    mFilesVector = getDataFileList();
    if (mFilesVector.empty()) {
      EDDLOG("(Sub)TimeFrame directory contains no data files.");
      return false;
    }
  } else {
    EDDLOG("(Sub)TimeFrame file source: TF location not specified.");
    return false;
  }

  if (mRegionSizeMB <= 0 || mHdrRegionSizeMB <= 0) {
    EDDLOG("(Sub)TimeFrame region sizes must not be zero.");
    return false;
  }

  if (mPreReadStfs == 0) {
    EDDLOG("Number of pre-read (Sub)TimeFrame must be >= 1.");
    return false;
  }

  {
    const auto lTfFilesVar = getenv("DATADIST_FILE_READ_COUNT");
    if (lTfFilesVar) {
      try {
        const auto lTfFilesNum = std::stol(lTfFilesVar);
        if (lTfFilesNum == 0) {
          WDDLOG("(Sub)TimeFrame source: DATADIST_FILE_READ_COUNT must be greater than 0.");
          return false;
        }

        mRepeat = true;
        mNumFiles = lTfFilesNum;
        WDDLOG("(Sub)TimeFrame source: DATADIST_FILE_READ_COUNT is set to {}.", lTfFilesNum);

      } catch (...) {
        EDDLOG("(Sub)TimeFrame source: DATADIST_FILE_READ_COUNT must be greater than 0. DATADIST_FILE_READ_COUNT={}",
          lTfFilesVar);
      }
    }
  }

  // print options
  IDDLOG("(Sub)TimeFrame source :: enabled                 = {}", (mEnabled ? "yes" : "no"));
  IDDLOG("(Sub)TimeFrame source :: file location           = {}", (mLocalFiles ? "local" : "remote"));
  if (mLocalFiles) {
    IDDLOG("(Sub)TimeFrame source :: directory               = {}", mDir);
  } else {
    IDDLOG("(Sub)TimeFrame source :: file list               = {}", mCopyFileList);
    IDDLOG("(Sub)TimeFrame source :: copy command            = {}", mCopyCmd);
  }
  IDDLOG("(Sub)TimeFrame source :: (s)tf load rate         = {}", mLoadRate);
  IDDLOG("(Sub)TimeFrame source :: (s)tf pre reads         = {}", mPreReadStfs);
  IDDLOG("(Sub)TimeFrame source :: repeat data             = {}", mRepeat);
  IDDLOG("(Sub)TimeFrame source :: num files in dataset    = {}", mFilesVector.size());
  IDDLOG("(Sub)TimeFrame source :: data region id          = {}", mTfDataRegionId.has_value() ? std::to_string(mTfDataRegionId.value()) : "");
  IDDLOG("(Sub)TimeFrame source :: data region size(MiB)   = {}", mRegionSizeMB);
  IDDLOG("(Sub)TimeFrame source :: header region id        = {}", mTfHdrRegionId.has_value() ? std::to_string(mTfHdrRegionId.value()) : "");
  IDDLOG("(Sub)TimeFrame source :: header region size(MiB) = {}", mHdrRegionSizeMB);

  return true;
}

// Fetch copy files if needed
void SubTimeFrameFileSource::DataFetcherThread()
{
  std::size_t lFileIndex = std::size_t(-1);
  std::uint64_t lFileErrors = 0;
  std::uint64_t lTotalFiles = 0;
  std::uint64_t lTotalSuccessfulFiles = 0;

  while (mRunning && lFileErrors < 10) {
    if (mInputFileQueue.size() > 4) {
      std::this_thread::sleep_for(5ms);
      continue;
    }

    // get the next list
    if (lTotalFiles >= mFilesVector.size() && !mRepeat) {
      IDDLOG("(Sub)TimeFrame source: finished loading all files. Waiting for injecting to complete.");
      break;
    }

    // make sure we progress on errors
    lFileIndex = (lFileIndex + 1) % mFilesVector.size();
    lTotalFiles++;

    if (mLocalFiles) {
      // simply forward
      mInputFileQueue.push(std::make_shared<StfFileMeta>(lFileIndex, mFilesVector[lFileIndex]));
    } else {
      // check if the file already exists in the cache
      auto lExisting = StfFileMeta::getExistingInstance(lFileIndex);
      if (lExisting) {
        mInputFileQueue.push(lExisting.value());
      } else {
        // run the copy command
        const auto lDstFileName = mCopyDstPath /
          ("cache-" + std::to_string(lFileIndex) + "-" + bfs::path(mFilesVector[lFileIndex]).filename().native());

        auto lRealCmd = boost::replace_all_copy(mCopyCmd, "?src", mFilesVector[lFileIndex]);
        boost::replace_all(lRealCmd, "?dst", lDstFileName.native());

        std::vector<std::string> lCopyParams { "-c", lRealCmd };
        bp::child lCopyChild(bp::search_path("sh"), lCopyParams, bp::std_err > mCopyCmdLogFile,
          bp::std_out > mCopyCmdLogFile);

        while (!lCopyChild.wait_for(5s)) {
          IDDLOG("(Sub)TimeFrame source: waiting for copy command. cmd='{}'", lRealCmd);
        }

        const auto lSysRet = lCopyChild.exit_code();
        if (lSysRet != 0) {
          WDDLOG_RL(1000, "(Sub)TimeFrame source: copy command returned non-zero exit code. cmd='{}' exit_code={}",
            lRealCmd, lSysRet);
        }

        if (!bfs::is_regular_file(lDstFileName) || bfs::is_empty(lDstFileName)) {
          EDDLOG("(Sub)TimeFrame source: copy command failed to fetch the file. stc_file={} dst_file={}.",
            mFilesVector[lFileIndex], lDstFileName.native());
          lFileErrors++;
          continue;
        }

        auto lNewFile = std::make_shared<StfFileMeta>(lFileIndex, lDstFileName.native(), true /* delete after done */);
        StfFileMeta::insertExistingInstance(lFileIndex, lNewFile);
        mInputFileQueue.push(lNewFile);
      }
    }

    lTotalSuccessfulFiles++;

    // check if we are done because of DATADIST_FILE_READ_COUNT
    if (mNumFiles > 0 && lTotalSuccessfulFiles == mNumFiles) {
      IDDLOG("(Sub)TimeFrame source: finished loading all files. DATADIST_FILE_READ_COUNT={}", mNumFiles);
      break;
    }
  }

  // close the file queue to signal the next thread to exit
  mInputFileQueue.stop();
  DDDLOG("Exiting file provider thread...");
}

/// File reading thread
void SubTimeFrameFileSource::DataHandlerThread()
{
  // inject rate
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0. ? unsigned(1000000. / mLoadRate) : 0);

  while (mRunning) {

    std::shared_ptr<StfFileMeta> lMyFile;
    if (!mRunning || !mInputFileQueue.pop(lMyFile)) {
      IDDLOG("(Sub)TimeFrame Source: Finished reading all input files. Exiting...");
      break;
    }
    assert(lMyFile);

    DDDLOG_RL(5000, "(Sub)TimeFrame Source: reading new file={}", lMyFile->mFilePath);
    auto lFileNameAbs = bfs::path(lMyFile->mFilePath);
    SubTimeFrameFileReader lStfReader(lFileNameAbs);

    try {
      // load multiple TF per file
      while (mRunning) {
        // read STF from file
        auto lStfPtr = lStfReader.read(*mFileBuilder);

        if (mRunning && lStfPtr) {
          // adapt Stf headers for different output channels, native or DPL
          mFileBuilder->adaptHeaders(lStfPtr.get());
          if (!mReadStfQueue.push(std::move(lStfPtr))) {
            break;
          }

        } else {
          // bad file?
          break; // EOF or !running
        }

        // Limit read-ahead
        while (mRunning && (mReadStfQueue.size() >= mPreReadStfs)) {
          std::this_thread::sleep_for(mPaused ? lIntervalUs : (lIntervalUs / 10));
        }
      }
    } catch (...) {
      EDDLOG("(Sub)TimeFrame Source: error while reading (S)TFs from file. file={} file_idx={}",
        lMyFile->mFilePath, lMyFile->mIdx);
    }

    // make sure we release the file first befor put call
    const auto lIdx = lMyFile->mIdx;
    lMyFile.reset();
    StfFileMeta::putExistingInstance(lIdx);
  }

  // notify the injection thread to stop
  mReadStfQueue.stop();

  DDDLOG("Exiting file source data load thread...");
}


/// STF injecting thread
void SubTimeFrameFileSource::DataInjectThread()
{
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0. ? unsigned(1000000. / mLoadRate) : 0);

  IDDLOG("(Sub)TimeFrame Source: Injecting new STF every {} us", lIntervalUs.count());

  static double sNumSentStfs = 1;

  while (mRunning) {

    // Get the next STF
    std::unique_ptr<SubTimeFrame> lStf;
    if (!mReadStfQueue.pop(lStf)) {
      break;
    }

    static auto sRateStartTime = std::chrono::high_resolution_clock::now();

    while (mRunning && mPaused) {
      std::this_thread::sleep_for(200ms);
      // reset the rate stats
      sRateStartTime = std::chrono::high_resolution_clock::now();
      sNumSentStfs = 1;
    }

    mPipelineI.queue(mPipelineStageOut, std::move(lStf));
    sNumSentStfs++;

    auto getElapsedTime = []() {
      return std::max(1e-6, std::chrono::duration<double>(std::chrono::high_resolution_clock::now() -
        sRateStartTime).count());
    };

    // rate limiting
    // calculate absolute times from the start to avoid skewing the rate over time
    while(mRunning) {
      const double lSecSinceStart = getElapsedTime();
      const double lSecNext = sNumSentStfs / mLoadRate;

      // check if we're done waiting
      if ((lSecNext - lSecSinceStart) < 0.001) {
        break;
      }

      // limit sleep time to 0.5s in order to be able to check for exit signal
      auto lWaitTime = std::clamp((lSecNext - lSecSinceStart) * 3. / 5., 0.001, 0.5);
      std::this_thread::sleep_for(std::chrono::duration<double>(lWaitTime));
    }
    DDDLOG_RL(2000, "SubTimeFrameFileSource prepared_tfs={} inject_rate={:.4f}",
      mReadStfQueue.size(), sNumSentStfs / getElapsedTime());
  }

  mPipelineI.close(mPipelineStageOut);

  DDDLOG("Exiting file source inject thread...");
}

}
} /* o2::DataDistribution */
