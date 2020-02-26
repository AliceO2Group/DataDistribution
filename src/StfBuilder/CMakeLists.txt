# @author Gvozden Neskovic
# @brief  cmake for StfBuilder

set(EXE_STFB_SOURCES
  StfBuilderInput
  StfBuilderDevice
  runStfBuilderDevice
)

add_executable(StfBuilder ${EXE_STFB_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(StfBuilder PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(StfBuilder
  PRIVATE
    base common
)

install(TARGETS StfBuilder RUNTIME DESTINATION bin)
