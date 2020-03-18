# @author Gvozden Neskovic
# @brief  cmake for ReadoutEmulator

set(EXE_EMU_SOURCES
  CruMemoryHandler
  CruEmulator
  ReadoutDevice
  runReadoutEmulatorDevice
)

add_executable(ReadoutEmulator ${EXE_EMU_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(ReadoutEmulator PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(ReadoutEmulator
  PRIVATE
    base common common_root
)

install(TARGETS ReadoutEmulator RUNTIME DESTINATION bin)
