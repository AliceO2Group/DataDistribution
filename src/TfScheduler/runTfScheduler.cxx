// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "TfSchedulerDevice.h"

#include <Config.h>

#include <options/FairMQProgOptions.h>
#include <runFairMQDevice.h>

namespace bpo = boost::program_options;
template class std::basic_string<char, std::char_traits<char>, std::allocator<char> >; // Workaround for bug in CC7 devtoolset7

void addCustomOptions(bpo::options_description& options)
{
  // Add options for Data Distribution discovery
  options.add(o2::DataDistribution::Config::getProgramOptions(o2::DataDistribution::ProcessType::TfSchedulerService));
}

FairMQDevicePtr getDevice(const FairMQProgOptions& /*config*/)
{
  return new o2::DataDistribution::TfSchedulerDevice();
}
