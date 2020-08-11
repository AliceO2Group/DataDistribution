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
#include <boost/program_options/options_description.hpp>
#include <boost/filesystem.hpp>

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

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileSource
////////////////////////////////////////////////////////////////////////////////

void SubTimeFrameFileSource::start(FairMQChannel& pDstChan, MemoryResources &pMemRes, const bool pDplEnabled)
{
  if (enabled()) {
    mDstChan = &pDstChan;
    mDplEnabled = pDplEnabled;

    mFileBuilder = std::make_unique<SubTimeFrameFileBuilder>(
      pDstChan,
      pMemRes,
      mRegionSizeMB << 20,
      mHdrRegionSizeMB << 20,
      mDplEnabled
    );

    mRunning = true;

    mInjectThread = create_thread_member("stf_file_inject", &SubTimeFrameFileSource::DataInjectThread, this);
    mSourceThread = create_thread_member("stf_file_read", &SubTimeFrameFileSource::DataHandlerThread, this);
  }
}

void SubTimeFrameFileSource::stop()
{
  mRunning = false;

  if (mFileBuilder) {
    mFileBuilder->stop();
  }

  mReadStfQueue.stop();
  mReadStfQueue.flush();

  if (mSourceThread.joinable()) {
    mSourceThread.join();
  }

  if (mInjectThread.joinable()) {
    mInjectThread.join();
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
    OptionKeyStfHeadersRegionSize,
    bpo::value<std::uint64_t>()->default_value(256),
    "Size of the memory region for (Sub)TimeFrames O2 headers in MiB. "
    "Note: make sure the region can fit several (Sub)TimeFrames to avoid deadlocks.");

  return lSinkDesc;
}

std::vector<std::string> SubTimeFrameFileSource::getDataFileList() const
{
  // Load the sorted list of StfFiles
  auto lFilesVector = FilePathUtils::getAllFiles(mDir);
  // Remove side-car files
  auto lRemIt = std::remove_if(lFilesVector.begin(), lFilesVector.end(),
    [](const std::string &lElem) {
      bool lToRemove =  boost::ends_with(lElem, ".info") || boost::ends_with(lElem, ".sh") || boost::starts_with(lElem, ".");

      DDLOGF(fair::Severity::DEBUG, "Checking if should remove file: {} ? {}", lElem, (lToRemove ? "yes" : "no"));
      return lToRemove;
    }
  );

  lFilesVector.erase(lRemIt, lFilesVector.end());

  return lFilesVector;
}

bool SubTimeFrameFileSource::loadVerifyConfig(const FairMQProgOptions& pFMQProgOpt)
{
  mEnabled = pFMQProgOpt.GetValue<bool>(OptionKeyStfSourceEnable);

  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame file source is {}", (mEnabled ? "enabled." : "disabled."));

  if (!mEnabled) {
    return true;
  }

  mDir = pFMQProgOpt.GetValue<std::string>(OptionKeyStfSourceDir);
  if (mDir.empty()) {
    DDLOGF(fair::Severity::ERROR, "(Sub)TimeFrame file source directory must be specified.");
    return false;
  }

  // make sure directory exists and it is readable
  bfs::path lDirPath(mDir);
  if (!bfs::is_directory(lDirPath)) {
    DDLOGF(fair::Severity::ERROR, "(Sub)TimeFrame file source directory does not exist.");
    return false;
  }

  mRepeat = pFMQProgOpt.GetValue<bool>(OptionKeyStfSourceRepeat);
  mLoadRate = pFMQProgOpt.GetValue<double>(OptionKeyStfLoadRate);
  mPreReadStfs = pFMQProgOpt.GetValue<std::uint32_t>(OptionKeyStfLoadPreRead);
  mRegionSizeMB = pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfSourceRegionSize);
  mHdrRegionSizeMB = pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfHeadersRegionSize);

  mFilesVector = getDataFileList();
  if (mFilesVector.empty()) {
    DDLOGF(fair::Severity::ERROR, "(Sub)TimeFrame directory contains no data files.");
    return false;
  }

  if (mRegionSizeMB <= 0 || mHdrRegionSizeMB <= 0) {
    DDLOGF(fair::Severity::ERROR, "(Sub)TimeFrame region sizes must not be zero.");
    return false;
  }

  if (mPreReadStfs == 0) {
    DDLOGF(fair::Severity::ERROR, "Number of pre-read (Sub)TimeFrame must be >= 1.");
    return false;
  }

  // print options
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: enabled                 = {}", (mEnabled ? "yes" : "no"));
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: directory               = {}", mDir);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: (s)tf load rate         = {}", mLoadRate);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: (s)tf pre reads         = {}", mPreReadStfs);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: repeat data             = {}", mRepeat);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: num files               = {}", mFilesVector.size());
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: data region size(MiB)   = {}", mRegionSizeMB);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: header region size(MiB) = {}", mHdrRegionSizeMB);

  return true;
}

/// STF injecting thread
void SubTimeFrameFileSource::DataInjectThread()
{
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0. ? unsigned(1000000. / mLoadRate) : 0);

  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame Source: Injecting new STF every {} us", lIntervalUs.count());

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
      if (lSecNext <= lSecSinceStart) {
        break;
      }

      std::this_thread::sleep_for(std::chrono::duration<double>( (lSecNext - lSecSinceStart) * 3. / 5. ));
    }
    DDLOGF_RL(2000, fair::Severity::DEBUG, "SubTimeFrameFileSource prepared_tfs={} inject_rate={:.4f}",
      mReadStfQueue.size(), sNumSentStfs / getElapsedTime());
  }

  DDLOGF(fair::Severity::INFO, "Exiting file source inject thread...");
}

/// File reading thread
void SubTimeFrameFileSource::DataHandlerThread()
{
  // inject rate
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0. ? unsigned(1000000. / mLoadRate) : 0);
  // Load the sorted list of StfFiles if empty
  if (mFilesVector.empty()) {
    mFilesVector = getDataFileList();
  }

  if (mFilesVector.empty()) {
    DDLOGF(fair::Severity::ERROR, "(Sub)TimeFrame directory contains no data files.");
    return;
  }

  while (mRunning) {

    for (const auto &lFileName : mFilesVector) {
      if (!mRunning) {
        break; // stop looping over files
      }

      auto lFileNameAbs = bfs::path(mDir) / bfs::path(lFileName);
      SubTimeFrameFileReader lStfReader(lFileNameAbs);

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
    }

    if (!mRepeat) {
      DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame Source: Finished reading all input files. Exiting...");
      break;
    }
  }
  DDLOGF(fair::Severity::INFO, "Exiting file source data load thread...");
}

}
} /* o2::DataDistribution */
