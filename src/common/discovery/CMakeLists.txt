


set (LIB_DISCOVERY_SOURCES
  ConfigConsul
  TfSchedulerRpcClient
  StfSenderRpcClient
)

set (LIB_DISCOVERY_PROTOS
  discovery.proto
)

protobuf_generate_cpp(PB_SRCS PB_HDRS ${LIB_DISCOVERY_PROTOS})
# GRPC_GENERATE_CPP(GRPC_SRCS GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR} ${LIB_DISCOVERY_PROTOS})



set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)



# get_filename_component(DISCOVERY_PROTO_PATH "${hw_proto}" PATH)
set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/discovery.grpc.pb.cc")
set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/discovery.grpc.pb.h")
add_custom_command(
      OUTPUT "${GRPC_SRCS}" "${GRPC_HDRS}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${CMAKE_CURRENT_SOURCE_DIR}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/${LIB_DISCOVERY_PROTOS}"
      DEPENDS "${LIB_DISCOVERY_PROTOS}"
)
        #-I "${hw_proto_path}"



add_library(discovery OBJECT ${LIB_DISCOVERY_SOURCES} ${PB_SRCS} ${PB_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})


# protobuf triggers warning, disable strict no-warning policy here
set_target_properties(discovery PROPERTIES
    COMPILE_FLAGS "-Wno-error -Wno-useless-cast"
)

set_target_properties(discovery PROPERTIES
    PB_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)


target_include_directories(discovery
  PUBLIC
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/discovery
    ${CMAKE_CURRENT_BINARY_DIR} # protobuf puts generated c++ files there
)

target_link_libraries(discovery
  PUBLIC
  PRIVATE
    ppconsul
    Boost::program_options
    FairMQ::FairMQ
    protobuf::libprotobuf
    gRPC::grpc++_unsecure
)
