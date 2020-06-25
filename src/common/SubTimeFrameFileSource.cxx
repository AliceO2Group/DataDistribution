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

void SubTimeFrameFileSource::start(FairMQChannel& pDstChan, const bool pDplEnabled)
{
  if (enabled()) {
    mDstChan = &pDstChan;
    mDplEnabled = pDplEnabled;

    if (!mFileBuilder) {
      mFileBuilder = std::make_unique<SubTimeFrameFileBuilder>(
        pDstChan,
        mRegionSizeMB << 20,
        mDplEnabled
      );
    }

    mRunning = true;
    mInjectThread = std::thread(&SubTimeFrameFileSource::DataInjectThread, this);
    mSourceThread = std::thread(&SubTimeFrameFileSource::DataHandlerThread, this);
  }
}

void SubTimeFrameFileSource::stop()
{
  mRunning = false;

  if (mFileBuilder) {
    mFileBuilder->stop();
  }

  mReadStfQueue.stop();

  if (mSourceThread.joinable()) {
    mSourceThread.join();
  }

  if (mInjectThread.joinable()) {
    mInjectThread.join();
  }

  mReadStfQueue.flush();

  mDstChan = nullptr;
  /* mFileBuilder = nullptr; // carrying the fmq memory resource, leave it alone */
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
    bpo::value<float>()->default_value(1),
    "Rate of injecting new (Sub)TimeFrames (approximate). -1 to inject as fast as possible.")(
    OptionKeyStfSourceRepeat,
    bpo::bool_switch()->default_value(false),
    "If enabled, repeatedly inject (Sub)TimeFrames into the chain.")(
    OptionKeyStfSourceRegionSize,
    bpo::value<std::uint64_t>()->default_value(1024),
    "Size of the memory region for (Sub)TimeFrames data in MiB. "
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

  DDLOG(fair::Severity::INFO) << "(Sub)TimeFrame file source is " << (mEnabled ? "enabled." : "disabled.");

  if (!mEnabled) {
    return true;
  }

  mDir = pFMQProgOpt.GetValue<std::string>(OptionKeyStfSourceDir);
  if (mDir.empty()) {
    DDLOG(fair::Severity::ERROR) << "(Sub)TimeFrame file source directory must be specified.";
    return false;
  }

  // make sure directory exists and it is readable
  bfs::path lDirPath(mDir);
  if (!bfs::is_directory(lDirPath)) {
    DDLOG(fair::Severity::ERROR) << "(Sub)TimeFrame file source directory does not exist.";
    return false;
  }

  mRepeat = pFMQProgOpt.GetValue<bool>(OptionKeyStfSourceRepeat);
  mLoadRate = pFMQProgOpt.GetValue<float>(OptionKeyStfLoadRate);
  mRegionSizeMB = pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfSourceRegionSize);

  mFilesVector = getDataFileList();
  if (mFilesVector.empty()) {
    DDLOGF(fair::Severity::ERROR, "(Sub)TimeFrame directory contains no data files.");
    return false;
  }

  if (mRegionSizeMB <= 0) {
    DDLOGF(fair::Severity::ERROR, "(Sub)TimeFrame region size must be a positive value");
    return false;
  }

  // print options
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: enabled         = {}", (mEnabled ? "yes" : "no"));
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: directory       = {}", mDir);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: (s)tf load rate = {}", mLoadRate);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: repeat data     = {}", mRepeat);
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: num files       = {}", mFilesVector.size());
  DDLOGF(fair::Severity::INFO, "(Sub)TimeFrame source :: region size(MiB)= {}", mRegionSizeMB);

  return true;
}

/// STF injecting thread
void SubTimeFrameFileSource::DataInjectThread()
{
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0.f ? unsigned(1000000.f / mLoadRate) : 0);

  DDLOG(fair::Severity::INFO) << "(Sub)TimeFrame Source: Injecting new STF every " << lIntervalUs.count() << " us";

  while (mRunning) {
    // Get the next STF
    std::unique_ptr<SubTimeFrame> lStf;
    if (!mReadStfQueue.pop(lStf)) {
      break;
    }

    mPipelineI.queue(mPipelineStageOut, std::move(lStf));
    auto lRatePrevTime = std::chrono::high_resolution_clock::now();

    while(mRunning && std::chrono::high_resolution_clock::now() - lRatePrevTime  < lIntervalUs) {

      auto lSleepFor = lIntervalUs - (std::chrono::high_resolution_clock::now() - lRatePrevTime);
      lSleepFor /= 2;
      lSleepFor = std::clamp(lSleepFor, 0ns, std::chrono::nanoseconds(500ms));

      std::this_thread::sleep_for(lSleepFor);
    }
  }

  DDLOG(fair::Severity::INFO) << "Exiting file source inject thread...";
}

/// File reading thread
void SubTimeFrameFileSource::DataHandlerThread()
{
  // inject rate
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0.f ? unsigned(1000000.f / mLoadRate) : 0);
  // Load the sorted list of StfFiles if empty
  if (mFilesVector.empty()) {
    mFilesVector = getDataFileList();
  }

  if (mFilesVector.empty()) {
    DDLOG(fair::Severity::ERROR) << "(Sub)TimeFrame directory contains no data files.";
    return;
  }

  std::uint32_t lCurrentOrbit = 0;

  while (mRunning) {

    for (const auto &lFileName : mFilesVector) {
      if (!mRunning) {
        break; // stop looping over files
      }

      auto lFileNameAbs = bfs::path(mDir) / bfs::path(lFileName);
      SubTimeFrameFileReader lStfReader(lFileNameAbs);

      // DDLOG(fair::Severity::DEBUG) << "FileSource: opened new file " << lFileNameAbs.string();

      while (mRunning) {
        // prevent large read-ahead
        while (mRunning && mReadStfQueue.size() > 3) {
          std::this_thread::sleep_for(lIntervalUs);
        }

        // read STF from file
        auto lStfPtr = lStfReader.read(*mFileBuilder);

        if (mRunning && lStfPtr) {
          // adapt Stf headers for different output channels, native or DPL
          mFileBuilder->adaptHeaders(lStfPtr.get());

          // TODO: timeframe id
          if (lStfPtr && mRepeat) {
            lStfPtr->setFirstOrbit(lCurrentOrbit);
            lStfPtr->updateStf();
            lCurrentOrbit += 256;
          }

          mReadStfQueue.push(std::move(lStfPtr));
        } else {
          // bad file?
          break; // EOF or !running
        }
      }
    }

    if (!mRepeat) {
      DDLOG(fair::Severity::INFO) << "(Sub)TimeFrame Source: Finished reading all input files. Exiting...";
      break;
    }
  }
  DDLOG(fair::Severity::INFO) << "Exiting file source data load thread...";
}

}
} /* o2::DataDistribution */
