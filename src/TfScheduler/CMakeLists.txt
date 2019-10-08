# @author Gvozden Neskovic
# @brief  cmake for TimeFrameScheduler (TfScheduler)

set(EXE_TFS_SOURCES
  TfSchedulerDevice
  TfSchedulerInstance
  TfSchedulerInstanceRpc
  TfSchedulerConnManager
  TfSchedulerTfBuilderInfo
  TfSchedulerStfInfo
  runTfScheduler
)

add_executable(TfScheduler ${EXE_TFS_SOURCES} $<TARGET_OBJECTS:discovery>)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(TfScheduler PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# get protobuf directory
get_target_property(PB_DIRECTORY discovery PB_DIRECTORY)

target_include_directories(TfScheduler
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include/discovery
    ${PB_DIRECTORY}
)

target_link_libraries(TfScheduler
  PUBLIC
  PRIVATE
    FairMQ::FairMQ
    ppconsul
    protobuf::libprotobuf
    gRPC::grpc++_unsecure
)

install(TARGETS TfScheduler RUNTIME DESTINATION bin)
