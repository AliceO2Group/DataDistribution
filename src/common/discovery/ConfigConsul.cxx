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

#include "ConfigParameters.h"
#include "ConfigConsul.h"

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <ppconsul/kv.h>

#include <string>
#include <chrono>
#include <iomanip>
#include <set>
#include <tuple>


namespace o2::DataDistribution
{

ProcessTypePB to_ProcessTypePB(const ProcessType pType) {
  static const std::map<ProcessType, ProcessTypePB> cProcessTypeMap =
  {
    { ProcessType::StfBuilder, StfBuilder },
    { ProcessType::StfSender, StfSender },
    { ProcessType::TfBuilder, TfBuilder },
    { ProcessType::TfScheduler, TfScheduler }
  };

  return cProcessTypeMap.at(pType);
}

namespace ConsulImpl {

std::string to_string(const ProcessTypePB pType) {

  switch (pType) {
    case StfBuilder: return "StfBuilder";
    case StfSender: return "StfSender";
    case TfBuilder: return "TfBuilder";
    case TfScheduler: return "TfScheduler";
    default: break;
  }
  assert(0 && "Should not reach");
  return std::string("UnknownType");
}

// human readable and milliseconds since epoch
std::tuple<std::string, std::uint64_t> getCurrentTimeString() {
  using namespace std::chrono;
  std::ostringstream ss;
  const auto lNow = system_clock::now();

  // get human readable string
  const auto lTimet = std::chrono::system_clock::to_time_t(lNow);
  ss << std::put_time(gmtime(&lTimet), "%FT%TZ");

  // get milliseconds
  const auto lSinceEpoch = lNow.time_since_epoch();
  const auto lMillis = std::chrono::duration_cast<std::chrono::milliseconds>(lSinceEpoch).count();

  return { ss.str(), lMillis };
}


} /* namespace ConsulImpl */

} /* namespace o2::DataDistribution */
