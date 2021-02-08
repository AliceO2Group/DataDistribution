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
#include "TfSchedulerDevice.h"
#include <Config.h>

#include <options/FairMQProgOptions.h>
#include <fairmq/DeviceRunner.h>

using namespace o2::DataDistribution;

int main(int argc, char* argv[])
{
    using namespace fair::mq;
    using namespace fair::mq::hooks;
    namespace bpo = boost::program_options;

    // set InfoLogger Facility
    DataDistLogger::sInfoLoggerFacility = "datadist/tfscheduler";

    fair::mq::DeviceRunner runner{argc, argv};

    // Populate options from the command line
    runner.AddHook<fair::mq::hooks::SetCustomCmdLineOptions>([](fair::mq::DeviceRunner& r) {

      // Add InfoLogger Options
      r.fConfig.AddToCmdLineOptions(impl::DataDistLoggerCtx::getProgramOptions());

      // Add options for Data Distribution discovery
      r.fConfig.AddToCmdLineOptions(
        o2::DataDistribution::Config::getProgramOptions(o2::DataDistribution::ProcessType::TfSchedulerService)
      );
    });


    runner.AddHook<InstantiateDevice>([](DeviceRunner& r){
      // r.fPluginManager.ForEachPlugin([](Plugin& p) {
      //   IDDLOG("Controlling pluggin: {}", p.GetName());
      // });

      // Install listener for Logging options
      o2::DataDistribution::impl::DataDistLoggerCtx::HandleFMQOptions(r);

      // Instantiate the device
      r.fDevice = std::make_unique<o2::DataDistribution::TfSchedulerDevice>();
    });

    return runner.RunWithExceptionHandlers();
}