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
      //   IDDLOG("Controlling pluggin: {}", p.GetName());
      // });

      // Install listener for Logging options
      o2::DataDistribution::impl::DataDistLoggerCtx::HandleFMQOptions(r);

      // Install listener for discovery partition key
      r.fConfig.Subscribe<std::string>("discovery-partition", [&](const std::string& pKey, std::string pValue) {

        if (pKey == "partition_id" || pKey == "partition-id" || pKey == "environment-id" || pKey == "environment_id") {

          if (r.fConfig.GetProperty<std::string>("discovery-partition") == "") {
            r.fConfig.SetProperty<std::string>("discovery-partition", pValue);
            IDDLOG("Config::Subscribe received key-value pair. {}=<{}>", pKey, pValue);
          }
        }
      });

      // reset unsupported options
      r.fConfig.SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 16u));
      r.fConfig.SetProperty<float>("rate", 0.f);
      r.fConfig.SetProperty<bool>("shm-throw-bad-alloc", false);
      r.fConfig.SetProperty<std::string>("transport", std::string("shmem"));
      r.fConfig.Notify();

      // Instantiate the device
      r.fDevice = std::make_unique<o2::DataDistribution::StfSenderDevice>();
    });

    return runner.RunWithExceptionHandlers();
}
