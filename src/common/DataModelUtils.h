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

#ifndef STF_DATAMODEL_UTILS_H_
#define STF_DATAMODEL_UTILS_H_

namespace o2::DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// IDataModelObject interface
////////////////////////////////////////////////////////////////////////////////
class EquipmentHBFrames;
class SubTimeFrame;

class ISubTimeFrameVisitor
{
 public:
  virtual void visit(SubTimeFrame&, void *p = nullptr) = 0;
};

class ISubTimeFrameConstVisitor
{
 public:
  virtual void visit(const SubTimeFrame&, void *p = nullptr) = 0;
};

class IDataModelObject
{
 public:
  virtual void accept(ISubTimeFrameVisitor& v, void *p = nullptr) = 0;
  virtual void accept(ISubTimeFrameConstVisitor& v, void *p = nullptr) const = 0;
};

} /* o2::DataDistribution */

#endif /* STF_DATAMODEL_UTILS_H_ */
