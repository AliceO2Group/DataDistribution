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
      mFileBuilder = std::make_unique<SubTimeFrameFileBuilder>(pDstChan, mDplEnabled);
    }

    mRunning = true;
    mInjectThread = std::thread(&SubTimeFrameFileSource::DataInjectThread, this);
    mSourceThread = std::thread(&SubTimeFrameFileSource::DataHandlerThread, this);
  }
}

void SubTimeFrameFileSource::stop()
{
  mRunning = false;

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
    bpo::value<std::uint64_t>()->default_value(44),
    "Rate of injecting new (Sub)TimeFrames (approximate). 0 to inject as fast as possible.")(
    OptionKeyStfSourceRepeat,
    bpo::bool_switch()->default_value(false),
    "If enabled, repeatedly inject (Sub)TimeFrames into the chain.");

  return lSinkDesc;
}

std::vector<std::string> SubTimeFrameFileSource::getDataFileList() const
{
  // Load the sorted list of StfFiles
  auto lFilesVector = FilePathUtils::getAllFiles(mDir);
  // Remove side-car files
  auto lRemIt = std::remove_if(lFilesVector.begin(), lFilesVector.end(),
    [](const std::string &lElem) {
      LOG(DEBUG) << "Checking if should remove file: " << lElem << " ? " << (boost::ends_with(lElem, ".info")? "yes" : "no");
      return boost::ends_with(lElem, ".info");
    }
  );

  lFilesVector.erase(lRemIt, lFilesVector.end());

  return lFilesVector;
}

bool SubTimeFrameFileSource::loadVerifyConfig(const FairMQProgOptions& pFMQProgOpt)
{
  mEnabled = pFMQProgOpt.GetValue<bool>(OptionKeyStfSourceEnable);

  LOG(INFO) << "(Sub)TimeFrame file source is " << (mEnabled ? "enabled." : "disabled.");

  if (!mEnabled) {
    return true;
  }

  mDir = pFMQProgOpt.GetValue<std::string>(OptionKeyStfSourceDir);
  if (mDir.empty()) {
    LOG(ERROR) << "(Sub)TimeFrame file source directory must be specified.";
    return false;
  }

  // make sure directory exists and it is readable
  bfs::path lDirPath(mDir);
  if (!bfs::is_directory(lDirPath)) {
    LOG(ERROR) << "(Sub)TimeFrame file source directory does not exist.";
    return false;
  }

  mRepeat = pFMQProgOpt.GetValue<bool>(OptionKeyStfSourceRepeat);
  mLoadRate = pFMQProgOpt.GetValue<std::uint64_t>(OptionKeyStfLoadRate);

  const auto lFilesVector = getDataFileList();
  if (lFilesVector.empty()) {
    LOG(ERROR) << "(Sub)TimeFrame directory contains no data files.";
    return false;
  }

  // print options
  LOG(INFO) << "(Sub)TimeFrame source :: enabled         = " << (mEnabled ? "yes" : "no");
  LOG(INFO) << "(Sub)TimeFrame source :: directory       = " << mDir;
  LOG(INFO) << "(Sub)TimeFrame source :: (s)tf load rate = " << mLoadRate;
  LOG(INFO) << "(Sub)TimeFrame source :: repeat data     = " << mRepeat;
  LOG(INFO) << "(Sub)TimeFrame source :: num files       = " << lFilesVector.size();

  return true;
}

/// STF injecting thread
void SubTimeFrameFileSource::DataInjectThread()
{
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0 ? 1000000 / mLoadRate : 0);

  LOG(INFO) << "(Sub)TimeFrame Source: Injecting new STF every " << lIntervalUs.count() << " us";

  auto lRatePrevTime = std::chrono::high_resolution_clock::now();

  while (mRunning) {
    // Get the next STF
    std::unique_ptr<SubTimeFrame> lStf;
    if (!mReadStfQueue.pop(lStf)) {
      break;
    }

    do {
      std::this_thread::sleep_for(5ms);
    } while(std::chrono::high_resolution_clock::now() - lRatePrevTime  < lIntervalUs);

    mPipelineI.queue(mPipelineStageOut, std::move(lStf));
    lRatePrevTime = std::chrono::high_resolution_clock::now();
  }

  LOG(INFO) << "Exiting file source inject thread...";
}

/// File reading thread
void SubTimeFrameFileSource::DataHandlerThread()
{
  // inject rate
  const std::chrono::microseconds lIntervalUs(mLoadRate > 0 ? 1000000 / mLoadRate : 0);
  // Load the sorted list of StfFiles
  auto lFilesVector = getDataFileList();
  if (lFilesVector.empty()) {
    LOG(ERROR) << "(Sub)TimeFrame directory contains no data files.";
    return;
  }

  while (mRunning) {

    for (const auto &lFileName : lFilesVector) {
      if (!mRunning) {
        break; // stop looping over files
      }

      auto lFileNameAbs = bfs::path(mDir) / bfs::path(lFileName);
      SubTimeFrameFileReader lStfReader(lFileNameAbs);

      LOG(DEBUG) << "FileSource: opened new file " << lFileNameAbs;

      while (mRunning) {
        // prevent large read-ahead
        while (mRunning && mReadStfQueue.size() > 3) {
          std::this_thread::sleep_for(lIntervalUs);
        }

        // read STF from file
        auto lStfPtr = lStfReader.read(*mDstChan);

        if (mRunning && lStfPtr) {
          // adapt Stf headers for different output channels, native or DPL
          mFileBuilder->adaptHeaders(lStfPtr.get());
          mReadStfQueue.push(std::move(lStfPtr));
        } else {
          break; // EOF or !running
        }
      }
    }

    if (!mRepeat) {
      LOG(INFO) << "(Sub)TimeFrame Source: Finished reading all input files. Exiting...";
      break;
    }
  }
  LOG(INFO) << "Exiting file source data load thread...";
}

}
} /* o2::DataDistribution */
