# @author Gvozden Neskovic
# @brief  cmake for SubTimeFrameSender

set(EXE_STFS_SOURCES
  StfSenderDevice
  StfSenderOutput
  StfSenderRpc
  runSubTimeFrameSenderDevice
)

add_executable(StfSender ${EXE_STFS_SOURCES} $<TARGET_OBJECTS:common> $<TARGET_OBJECTS:discovery>)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(StfSender PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# get protobuf directory
get_target_property(PB_DIRECTORY discovery PB_DIRECTORY)

target_include_directories(StfSender
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include/discovery
    ${PB_DIRECTORY}
)

target_link_libraries(StfSender
  PUBLIC
  PRIVATE
    FairMQ::FairMQ
    ppconsul
    protobuf::libprotobuf
    gRPC::grpc++_unsecure
    AliceO2::Headers
    ROOT::Gui
)

install(TARGETS StfSender RUNTIME DESTINATION bin)







