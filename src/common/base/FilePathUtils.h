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

#ifndef FILE_PATH_UTILS_H_
#define FILE_PATH_UTILS_H_

#include <vector>

namespace o2::DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// FilePathUtils class
////////////////////////////////////////////////////////////////////////////////

class FilePathUtils
{
 public:
  FilePathUtils() = delete;

  static std::string getDataDirName(const std::string& pRoot);
  static std::string getNextSeqName(const std::string& pRoot);

  static std::vector<std::string> getAllFiles(const std::string& pDir);
};

} /* o2::DataDistribution */

#endif /* FILE_PATH_UTILS_H_ */
