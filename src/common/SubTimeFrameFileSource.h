// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

#ifndef ALICEO2_SUBTIMEFRAME_FILE_SOURCE_H_
#define ALICEO2_SUBTIMEFRAME_FILE_SOURCE_H_

#include "ConcurrentQueue.h"
#include "SubTimeFrameBuilder.h"

#include "DataDistLogger.h"

#include <boost/program_options/options_description.hpp>
#include <boost/filesystem.hpp>

#include <fstream>
#include <vector>

namespace o2::DataDistribution
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

  struct StfFileMeta {
    static std::mutex sLiveFilesLock;
    static std::map<std::size_t, std::shared_ptr<StfFileMeta>> mLiveFiles;

    static std::optional<std::shared_ptr<StfFileMeta>> getExistingInstance(const std::size_t pIdx) {
      std::scoped_lock lLock(sLiveFilesLock);
      if (mLiveFiles.count(pIdx) == 1) {
        return mLiveFiles[pIdx];
      }

      return std::nullopt;
    }

    static void insertExistingInstance(const std::size_t pIdx, std::shared_ptr<StfFileMeta> pFile) {
      std::scoped_lock lLock(sLiveFilesLock);
      assert (mLiveFiles.count(pIdx) == 0);
      mLiveFiles[pIdx] = pFile;
    }

    static void putExistingInstance(const std::size_t pIdx) {
      std::scoped_lock lLock(sLiveFilesLock);

      // The cache will not have an entry for local files
      // assert( mLiveFiles.count(pIdx) == 1 );

      if (mLiveFiles.count(pIdx) == 1) {
        if (mLiveFiles[pIdx].use_count() == 1) {
          mLiveFiles.erase(pIdx);
        }
      }
    }

    StfFileMeta() = delete;
    StfFileMeta(const std::size_t pIdx, const std::string &pPath, bool pDelete = false)
    : mFilePath(pPath), mIdx(pIdx), mDeleteMe(pDelete) { }

    StfFileMeta(const StfFileMeta &) = delete;

    ~StfFileMeta() {
      if (mDeleteMe) {
        try {
          boost::filesystem::remove(mFilePath);
        } catch(...) { }
      }
    }

    std::string mFilePath;
    std::size_t mIdx;
    bool mDeleteMe;
  };

 public:
  static constexpr const char* OptionKeyStfSourceEnable = "data-source-enable";
  static constexpr const char* OptionKeyStfSourceDir = "data-source-dir";
  static constexpr const char* OptionKeyStfLoadRate = "data-source-rate";
  static constexpr const char* OptionKeyStfLoadPreRead = "data-source-preread";
  static constexpr const char* OptionKeyStfSourceRepeat = "data-source-repeat";
  static constexpr const char* OptionKeyStfSourceRegionSize = "data-source-regionsize";
  static constexpr const char* OptionKeyStfSourceRegionId = "data-source-region-shmid";
  static constexpr const char* OptionKeyStfHeadersRegionSize = "data-source-headersize";
  static constexpr const char* OptionKeyStfHeadersRegionId = "data-source-header-shmid";

  static constexpr const char* OptionKeyStfFileList = "data-source-file-list";
  static constexpr const char* OptionKeyStfCopyCmd = "data-source-copy-cmd";


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

  bool loadVerifyConfig(const fair::mq::ProgOptions& pFMQProgOpt);
  std::vector<std::string> getDataFileList() const;

  bool enabled() const { return mEnabled; }

  void start(SyncMemoryResources &pMemRes);
  void pause() { mPaused = true; }
  void resume() { mPaused = false; }
  void stop();

  void DataFetcherThread();
  void DataHandlerThread();
  void DataInjectThread();

 private:
  stf_pipeline& mPipelineI;

  unsigned mPipelineStageOut;
  std::unique_ptr<SubTimeFrameFileBuilder> mFileBuilder;

  /// File feed pipe
  ConcurrentFifo<std::shared_ptr<StfFileMeta>> mInputFileQueue;

  /// Configuration
  bool mEnabled = false;
  std::string mDir;
  std::vector<std::string> mFilesVector;
  bool mRepeat = false;
  std::size_t mNumFiles = 0;
  bool mLocalFiles = true;

  std::string mCopyFileList;
  std::string mCopyCmd;
  std::string mCopyCmdLogFile;
  boost::filesystem::path mCopyDstPath;

  double mLoadRate = 1.f;
  std::uint32_t mPreReadStfs = 1;
  std::size_t mRegionSizeMB = 1024; /* 1GB in MiB */
  std::optional<std::uint16_t> mTfDataRegionId = std::nullopt;
  std::size_t mHdrRegionSizeMB = 256;
  std::optional<std::uint16_t> mTfHdrRegionId = std::nullopt;

  /// Thread for file writing
  std::atomic_bool mRunning = false;
  std::atomic_bool mPaused = false;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mReadStfQueue;

  std::thread mFetchThread;
  std::thread mSourceThread;
  std::thread mInjectThread;
};

} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_SOURCE_H_ */
