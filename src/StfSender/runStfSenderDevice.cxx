// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "DataDistLogger.h"
#include "StfSenderDevice.h"
#include <SubTimeFrameFileSink.h>
#include <Config.h>

#include <options/FairMQProgOptions.h>

namespace bpo = boost::program_options;

using namespace o2::DataDistribution;

int main(int argc, char* argv[])
{
    using namespace fair::mq;
    using namespace fair::mq::hooks;
    namespace bpo = boost::program_options;

    // set InfoLogger Facility
    DataDistLogger::sInfoLoggerFacility = "datadist/stfsender";

    fair::mq::DeviceRunner runner{argc, argv};

    // Populate options from the command line
    runner.AddHook<fair::mq::hooks::SetCustomCmdLineOptions>([](fair::mq::DeviceRunner& r) {

      // Add InfoLogger Options
      r.fConfig.AddToCmdLineOptions(impl::DataDistLoggerCtx::getProgramOptions());

      // StfSender options
      bpo::options_description lStfSenderOptions("StfSender options", 120);

      lStfSenderOptions.add_options()(
        o2::DataDistribution::StfSenderDevice::OptionKeyInputChannelName,
        bpo::value<std::string>()->default_value("builder-stf-channel"),
        "Name of the input STF channel")(
        o2::DataDistribution::StfSenderDevice::OptionKeyStandalone,
        bpo::bool_switch()->default_value(false),
        "Standalone operation. SubTimeFrames will not be forwarded to other processes.")(
        o2::DataDistribution::StfSenderDevice::OptionKeyMaxBufferedStfs,
        bpo::value<std::int64_t>()->default_value(-1),
        "Maximum number of buffered SubTimeFrames before starting to drop data. "
        "Unlimited: -1.");

      r.fConfig.AddToCmdLineOptions(lStfSenderOptions);

      // Add options for Data Distribution discovery
      r.fConfig.AddToCmdLineOptions(
          o2::DataDistribution::Config::getProgramOptions(o2::DataDistribution::ProcessType::StfSender)
      );

      // Add options for STF file sink
      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::SubTimeFrameFileSink::getProgramOptions());

    });

    runner.AddHook<InstantiateDevice>([](DeviceRunner& r){
      // r.fPluginManager.ForEachPlugin([](Plugin& p) {
      //   DDLOGF(DataDistSeverity::INFO, "Controlling pluggin: {}", p.GetName());
      // });

      // Install listener for Logging options
      o2::DataDistribution::impl::DataDistLoggerCtx::HandleFMQOptions(r);

      // Instantiate the device
      r.fDevice = std::make_unique<o2::DataDistribution::StfSenderDevice>();
    });

    return runner.RunWithExceptionHandlers();
}
