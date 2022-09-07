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

#ifndef ALICEO2_DATADIST_CONFIG_PARAMETERS_H_
#define ALICEO2_DATADIST_CONFIG_PARAMETERS_H_

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "discovery.pb.h"
#pragma GCC diagnostic pop


#include <string>
#include <vector>
#include <cassert>

namespace o2::DataDistribution
{

enum TfBuilderState {
  Initialized,
  Connecting,
  Connected,
  Running
};

inline
std::string to_string(const TfBuilderState s) {
  switch (s) {
    case Initialized: return "initialized";
    case Connecting: return "connecting";
    case Connected: return "connected";
    case Running: return "running";
    default: return "unknown";
  }
}

struct PartitionRequest {
  std::string   mPartitionId;
  std::string   mReqCreatedTime;
  std::vector<std::string> mStfSenderIdList;

  PartitionParameters mParameters;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_CONFIG_PARAMETERS_H_ */
