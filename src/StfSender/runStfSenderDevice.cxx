// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "StfSenderDevice.h"
#include <SubTimeFrameFileSink.h>
#include <Config.h>

#include <options/FairMQProgOptions.h>
#include <runFairMQDevice.h>

namespace bpo = boost::program_options;
template class std::basic_string<char, std::char_traits<char>, std::allocator<char> >; // Workaround for bug in CC7 devtoolset7

void addCustomOptions(bpo::options_description& options)
{
  options.add_options()(
    o2::DataDistribution::StfSenderDevice::OptionKeyInputChannelName,
    bpo::value<std::string>()->default_value("builder-stf-channel"),
    "Name of the input STF channel")(
    o2::DataDistribution::StfSenderDevice::OptionKeyStandalone,
    bpo::bool_switch()->default_value(false),
    "Standalone operation. SubTimeFrames will not be forwarded to other processes.")(
    o2::DataDistribution::StfSenderDevice::OptionKeyMaxBufferedStfs,
    bpo::value<std::int64_t>()->default_value(-1),
    "Maximum number of buffered SubTimeFrames before starting to drop data. "
    "Unlimited: -1.")(
    o2::DataDistribution::StfSenderDevice::OptionKeyGui,
    bpo::bool_switch()->default_value(false),
    "Enable GUI.");

  // Add options for STF file sink
  options.add(o2::DataDistribution::SubTimeFrameFileSink::getProgramOptions());
  // Add options for Data Distribution discovery
  options.add(o2::DataDistribution::Config::getProgramOptions(o2::DataDistribution::ProcessType::StfSender));
}

FairMQDevicePtr getDevice(const FairMQProgOptions& /*config*/)
{
  return new o2::DataDistribution::StfSenderDevice();
}
