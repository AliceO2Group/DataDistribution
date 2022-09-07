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

#include "StfBuilderDevice.h"
#include <fairmq/ProgOptions.h>
#include <grpc/grpc.h>

#include <SubTimeFrameFileSink.h>
#include <SubTimeFrameFileSource.h>
#include <FmqUtilities.h>
#include <Config.h>

#include <fairmq/DeviceRunner.h>

#include "DataDistLogger.h"

using namespace o2::DataDistribution;

int main(int argc, char* argv[])
{
    using namespace fair::mq;
    using namespace fair::mq::hooks;
    namespace bpo = boost::program_options;

    grpc_init();

    // set InfoLogger Facility
    DataDistLogger::sInfoLoggerFacility = "datadist/stfbuilder";

    fair::mq::DeviceRunner runner{argc, argv};

    // Populate options from the command line
    runner.AddHook<fair::mq::hooks::SetCustomCmdLineOptions>([](fair::mq::DeviceRunner& r) {

      // Add InfoLogger Options
      r.fConfig.AddToCmdLineOptions(impl::DataDistLoggerCtx::getProgramOptions());
      // Add Monitoring Options
      r.fConfig.AddToCmdLineOptions(DataDistMonitor::getProgramOptions());

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
          o2::DataDistribution::StfBuilderDevice::OptionKeyMaxBuiltStfs,
          bpo::value<std::uint64_t>()->default_value(0),
          "Maximum number of built and forwarded (Sub)TimeFrames before closing (unlimited: 0, default)."
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

      r.fConfig.AddToCmdLineOptions(lStfBuilderOptions);
      r.fConfig.AddToCmdLineOptions(lStfBuilderDplOptions);

      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::StfBuilderDevice::getDetectorProgramOptions());
      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::StfBuilderDevice::getStfBuildingProgramOptions());

      // Add options for STF file sink
      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::SubTimeFrameFileSink::getProgramOptions());
      // Add options for STF file source
      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::SubTimeFrameFileSource::getProgramOptions());
      // Add options for Data Distribution discovery
      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::Config::getProgramOptionsStfBuilder());

    });

    runner.AddHook<InstantiateDevice>([](DeviceRunner& r){

      // Install listener for Logging options
      fmqtools::HandleFMQOptions(r);

      // reset unsupported options
      r.fConfig.SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 4u));
      r.fConfig.SetProperty<float>("rate", 0.f);
      r.fConfig.SetProperty<bool>("shm-throw-bad-alloc", false);
      r.fConfig.SetProperty<std::string>("transport", std::string("shmem"));
      r.fConfig.Notify();

      // Instantiate the device
      r.fDevice = std::make_unique<o2::DataDistribution::StfBuilderDevice>();
    });

    auto lRet = runner.RunWithExceptionHandlers();

    grpc_shutdown();

    return lRet;
}
