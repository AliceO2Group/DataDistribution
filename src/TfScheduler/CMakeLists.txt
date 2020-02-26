# @author Gvozden Neskovic
# @brief  cmake for TfScheduler

set(EXE_TFS_SOURCES
  TfSchedulerDevice
  TfSchedulerInstance
  TfSchedulerInstanceRpc
  TfSchedulerConnManager
  TfSchedulerTfBuilderInfo
  TfSchedulerStfInfo
  runTfScheduler
)

add_executable(TfScheduler ${EXE_TFS_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(TfScheduler PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(TfScheduler
  PRIVATE
    base discovery
)

install(TARGETS TfScheduler RUNTIME DESTINATION bin)
