# @author Gvozden Neskovic
# @brief  Datadist Contoller, protobuf, and gRPC

#-------------------------------------------------------------------------------


set (LIB_DDCONTROL_SOURCES
)

set (LIB_DDCONTROL_PROTOS
  DataDistControl.proto
)

protobuf_generate_cpp(PB_SRCS PB_HDRS ${LIB_DDCONTROL_PROTOS})


set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)


if(APPLE)
  set(gRPC_LIB_DIR DYLD_LIBRARY_PATH=$<TARGET_FILE_DIR:gRPC::grpc++>:$<TARGET_FILE_DIR:protobuf::libprotobuf>)
endif()

# get_filename_component(DISCOVERY_PROTO_PATH "${hw_proto}" PATH)
set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/DataDistControl.grpc.pb.cc")
set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/DataDistControl.grpc.pb.h")
add_custom_command(
      OUTPUT "${GRPC_SRCS}" "${GRPC_HDRS}"
      COMMAND ${gRPC_LIB_DIR} ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${CMAKE_CURRENT_SOURCE_DIR}"
        --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
        "${CMAKE_CURRENT_SOURCE_DIR}/${LIB_DDCONTROL_PROTOS}"
      DEPENDS ${LIB_DDCONTROL_PROTOS} protobuf::protoc gRPC::grpc_cpp_plugin
      VERBATIM
)


add_library(ddcontrol OBJECT ${LIB_DDCONTROL_SOURCES} ${PB_SRCS} ${PB_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})

# protobuf triggers warning, disable strict no-warning policy here
if(NOT APPLE)
  set_target_properties(ddcontrol PROPERTIES
      COMPILE_FLAGS "-Wno-error -Wno-useless-cast"
  )
endif()

set_target_properties(ddcontrol PROPERTIES
    PB_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

target_include_directories(ddcontrol
  PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR} # protobuf puts generated c++ files there
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(ddcontrol
  PUBLIC
    ppconsul
    protobuf::libprotobuf
    gRPC::grpc++_unsecure
  PRIVATE
    base
    FairMQ::FairMQ
    Boost::program_options
)

