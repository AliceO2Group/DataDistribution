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

#include "StfBuilderDevice.h"
#include <options/FairMQProgOptions.h>

#include <SubTimeFrameFileSink.h>
#include <SubTimeFrameFileSource.h>

#include <Headers/DataHeader.h>

#include <runFairMQDevice.h>

namespace bpo = boost::program_options;
template class std::basic_string<char, std::char_traits<char>, std::allocator<char> >; // Workaround for bug in CC7 devtoolset7

void addCustomOptions(bpo::options_description& options)
{

  bpo::options_description lStfBuilderOptions("StfBuilder options", 120);

  lStfBuilderOptions.add_options()
    (
      o2::DataDistribution::StfBuilderDevice::OptionKeyInputChannelName,
      bpo::value<std::string>()->default_value("readout"),
      "Name of the readout channel (input)."
    )
    (
      o2::DataDistribution::StfBuilderDevice::OptionKeyStandalone,
      bpo::bool_switch()->default_value(false),
      "Standalone operation. SubTimeFrames will not be forwarded to other processes."
    )
    (
      o2::DataDistribution::StfBuilderDevice::OptionKeyMaxBufferedStfs,
      bpo::value<std::int64_t>()->default_value(-1),
      "Maximum number of buffered SubTimeFrames before starting to drop data (unlimited: -1)."
    )
    (
      o2::DataDistribution::StfBuilderDevice::OptionKeyOutputChannelName,
      bpo::value<std::string>()->default_value("builder-stf-channel"),
      "Name of the output channel."
    );

  bpo::options_description lStfBuilderDplOptions("StfBuilder DPL options", 120);
  lStfBuilderDplOptions.add_options()
  (
    o2::DataDistribution::StfBuilderDevice::OptionKeyDplChannelName,
    bpo::value<std::string>()->default_value(""),
    "Name of the dpl output channel. If empty, skip the DPL and connect to StfSender."
  );

  options.add(lStfBuilderOptions);
  options.add(lStfBuilderDplOptions);

  options.add(o2::DataDistribution::StfBuilderDevice::getDetectorProgramOptions());
  options.add(o2::DataDistribution::StfBuilderDevice::getStfBuildingProgramOptions());

  // Add options for STF file sink
  options.add(o2::DataDistribution::SubTimeFrameFileSink::getProgramOptions());
  // Add options for STF file source
  options.add(o2::DataDistribution::SubTimeFrameFileSource::getProgramOptions());
}

FairMQDevicePtr getDevice(const FairMQProgOptions& /*config*/)
{
  return new o2::DataDistribution::StfBuilderDevice();
}
