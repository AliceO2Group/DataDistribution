# @author Gvozden Neskovic
# @brief  cmake for StfSender

set(EXE_STFS_SOURCES
  StfSenderDevice
  StfSenderOutput
  StfSenderRpc
  runStfSenderDevice
)

add_executable(StfSender ${EXE_STFS_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(StfSender PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(StfSender
  PRIVATE
    base common common_root discovery
)

install(TARGETS StfSender RUNTIME DESTINATION bin)
