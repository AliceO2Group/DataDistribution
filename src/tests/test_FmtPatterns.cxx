// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#define BOOST_TEST_MAIN
#define BOOST_TEST_MODULE "FmtLib"

#include <boost/test/unit_test.hpp>
#include <DataDistLogger.h>

#include <iostream>

using namespace fmt;
using namespace o2::DataDistribution;

static const constexpr char* FmtSubSpec = "{:#06x}";

BOOST_AUTO_TEST_CASE(GetNextSeqNameTest)
{
  std::cerr << format(FmtSubSpec, 0) << std::endl;

  BOOST_CHECK("0x0000" == format(FmtSubSpec, 0)); // 32bit
  BOOST_CHECK("0xff00" == format(FmtSubSpec, 0xFF00)); // 32bit
  BOOST_CHECK("0x00ff" == format(FmtSubSpec, 0x00FF)); // 32bit



  BOOST_CHECK("0001" == format("{:04}", 1)); // leading zeroes


  DataDistLogger(DataDistSeverity::info, DataDistLogger::log_fmq{}, std::string("{}"));

  IDDLOG("Test {} {} {}", 1, 2);
}
