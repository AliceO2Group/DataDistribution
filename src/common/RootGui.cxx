// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "RootGui.h"

namespace o2
{
namespace DataDistribution
{

RootGui::RootGui(const std::string& pName, const std::string& pWindowName,
                 const unsigned pW, const unsigned pH)
{
  mApp = std::make_unique<TApplication>(pName.c_str(), nullptr, nullptr);

  mCanvas = std::make_unique<TCanvas>("cnv", pWindowName.c_str(), pW, pH);
  mCanvas->GetCanvas()->SetBit(kNoContextMenu);
}
}
} /* o2::DataDistribution */
