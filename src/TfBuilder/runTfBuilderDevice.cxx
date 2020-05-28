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

#include "TfBuilderDevice.h"

#include <Config.h>
#include <SubTimeFrameFileSink.h>

#include <options/FairMQProgOptions.h>
#include <runFairMQDevice.h>

namespace bpo = boost::program_options;
template class std::basic_string<char, std::char_traits<char>, std::allocator<char> >; // Workaround for bug in CC7 devtoolset7

void addCustomOptions(bpo::options_description& options)
{

  bpo::options_description lTfBuilderOptions("TfBuilder options", 120);

  lTfBuilderOptions.add_options()(
    o2::DataDistribution::TfBuilderDevice::OptionKeyStandalone,
    bpo::bool_switch()->default_value(false),
    "Standalone operation. TimeFrames will not be forwarded to other processes.")(
    o2::DataDistribution::TfBuilderDevice::OptionKeyTfMemorySize,
    bpo::value<std::uint64_t>()->default_value(512),
    "Memory buffer reserved for building and buffering TimeFrames (in MiB).");

  bpo::options_description lTfBuilderDplOptions("TfBuilder DPL options", 120);
  lTfBuilderDplOptions.add_options()
  (
    o2::DataDistribution::TfBuilderDevice::OptionKeyDplChannelName,
    bpo::value<std::string>()->default_value(""),
    "Name of the DPL output channel."
  );

  options.add(lTfBuilderOptions);
  options.add(lTfBuilderDplOptions);

  // Add options for TF file sink
  options.add(o2::DataDistribution::SubTimeFrameFileSink::getProgramOptions());
  // Add options for Data Distribution discovery
  options.add(o2::DataDistribution::Config::getProgramOptions(o2::DataDistribution::ProcessType::TfBuilder));
}

FairMQDevicePtr getDevice(const FairMQProgOptions& /*config*/)
{
  return new o2::DataDistribution::TfBuilderDevice();
}
