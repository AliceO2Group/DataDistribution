# Copyright 2019-2022 CERN and copyright holders of ALICE O2.
# See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
# All rights not expressly granted are reserved.
#
# This software is distributed under the terms of the GNU General Public
# License v3 (GPL Version 3), copied verbatim in the file "COPYING".
#
# In applying this license CERN does not waive the privileges and immunities
# granted to it by virtue of its status as an Intergovernmental Organization
# or submit itself to any jurisdiction.

## \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt
## \brief  Service discovery, protobuf, and gRPC

#-------------------------------------------------------------------------------

set (LIB_DISCOVERY_SOURCES
  ConfigConsul.cxx
)

set (LIB_DISCOVERY_PROTOS
  discovery.proto
)

protobuf_generate_cpp(PB_SRCS PB_HDRS ${LIB_DISCOVERY_PROTOS})


set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)


if(APPLE)
  set(gRPC_LIB_DIR DYLD_LIBRARY_PATH=$<TARGET_FILE_DIR:gRPC::grpc++>:$<TARGET_FILE_DIR:protobuf::libprotobuf>)
endif()

# get_filename_component(DISCOVERY_PROTO_PATH "${hw_proto}" PATH)
set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/discovery.grpc.pb.cc")
set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/discovery.grpc.pb.h")
add_custom_command(
      OUTPUT "${GRPC_SRCS}" "${GRPC_HDRS}"
      COMMAND ${gRPC_LIB_DIR} ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${CMAKE_CURRENT_SOURCE_DIR}"
        --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
        "${CMAKE_CURRENT_SOURCE_DIR}/${LIB_DISCOVERY_PROTOS}"
      DEPENDS ${LIB_DISCOVERY_PROTOS} protobuf::protoc gRPC::grpc_cpp_plugin
      VERBATIM
)


add_library(discovery OBJECT ${LIB_DISCOVERY_SOURCES} ${PB_SRCS} ${PB_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})

# protobuf triggers warning, disable strict no-warning policy here
if(NOT APPLE)
  set_target_properties(discovery PROPERTIES
      COMPILE_FLAGS "-Wno-error -Wno-useless-cast"
  )
endif()

set_target_properties(discovery PROPERTIES
    PB_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

target_include_directories(discovery
  PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR} # protobuf puts generated c++ files there
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(discovery
  PUBLIC
    ppconsul
    protobuf::libprotobuf
    gRPC::grpc++_unsecure
    FairMQ::FairMQ
  PRIVATE
    base
    Boost::program_options
)
