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

add_library(TfScheduler_lib OBJECT ${EXE_TFS_SOURCES})
target_link_libraries(TfScheduler_lib  base discovery)

add_executable(TfScheduler)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(TfScheduler PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(TfScheduler
  PRIVATE
    TfScheduler_lib
    base discovery
)

install(TARGETS TfScheduler RUNTIME DESTINATION bin)
