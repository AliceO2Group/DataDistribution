# @author Gvozden Neskovic
# @brief  cmake for StfSender

set(EXE_STFS_SOURCES
  StfSenderDevice
  StfSenderOutput
  StfSenderOutputFairMQ
  StfSenderRpc
  runStfSenderDevice
)

add_library(StfSender_lib OBJECT ${EXE_STFS_SOURCES})
target_link_libraries(StfSender_lib  base fmqtools common discovery monitoring)

add_executable(StfSender)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(StfSender PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(StfSender
  PRIVATE
    StfSender_lib
    base fmqtools common discovery monitoring
)

install(TARGETS StfSender RUNTIME DESTINATION bin)
