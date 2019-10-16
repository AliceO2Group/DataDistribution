# @author Gvozden Neskovic
# @brief  cmake for TimeFrameBuilder

set(EXE_TFB_SOURCES
  TfBuilderDevice
  TfBuilderInput
  TfBuilderRpc
  runTimeFrameBuilderDevice
)

add_executable(TfBuilder ${EXE_TFB_SOURCES} $<TARGET_OBJECTS:common> $<TARGET_OBJECTS:discovery>)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(TfBuilder PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# get protobuf directory
get_target_property(PB_DIRECTORY discovery PB_DIRECTORY)

target_include_directories(TfBuilder
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include/discovery
    ${PB_DIRECTORY}
)

target_link_libraries(TfBuilder
  PUBLIC
  PRIVATE
    FairMQ::FairMQ
    ppconsul
    protobuf::libprotobuf
    gRPC::grpc++_unsecure
    AliceO2::Headers
    AliceO2::Framework
    ROOT::Gui
)

install(TARGETS TfBuilder RUNTIME DESTINATION bin)
