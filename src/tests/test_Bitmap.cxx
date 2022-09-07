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

#define BOOST_TEST_MAIN
#define BOOST_TEST_MODULE "Bitmap"

#include <boost/test/unit_test.hpp>
#include <DataDistLogger.h>

#include <iostream>

using namespace o2::DataDistribution;



BOOST_AUTO_TEST_CASE(GetNextSeqNameTest)
{

  std::cout << sizeof (TokenBitfield<220>) << " " << sizeof (TokenBitfield<256>) << std::endl;

  {
    TokenBitfield<220> lField1;
    TokenBitfield<220> lField2;

    BOOST_CHECK(lField1.empty());
    BOOST_CHECK(lField2.empty());

    lField1.set_all();
    BOOST_CHECK(!lField1.empty());

    BOOST_CHECK(lField1.first() == 1);
    BOOST_CHECK(lField2.first() == TokenBitfield<220>::sInvalidIdx);


    lField2.set(23);
    lField1 &= lField2;
    BOOST_CHECK(lField1.first() == 23);
  }

  {
    TokenBitfield<256> lField1;
    TokenBitfield<256> lField2;

    lField1.set(53);
    lField1.set(73);
    lField1.set(196);
    lField1.set(197);
    lField1.set(198);
    lField1.set(199);
    lField1.set(200);
    lField1.set(201);
    lField1.set(202);

    lField2.set(23);
    lField2.set(73);
    lField2.set(196);
    lField2.set(197);
    lField2.set(198);
    lField2.set(199);
    lField2.set(200);
    lField2.set(201);
    lField2.set(202);

    lField2 &= lField1;

    // lField2.set_all();

    int hist[256] = { 0 };

    BOOST_CHECK(lField2.get(73));



    for (int i = 0; i < 100000; i ++) {
      hist[lField2.random_idx() - 1] += 1;
    }


    for (int i = 0; i < 256; i ++) {
      if (hist[i]) {
        std::cout << "hist[" << i << "] = " << hist[i] << std::endl;
      }
    }

    BOOST_CHECK(true);
  }


  // std::cerr << format(FmtSubSpec, 0) << std::endl;
  // DataDistLogger(DataDistSeverity::info, DataDistLogger::log_fmq{}, std::string("{}"));
}
