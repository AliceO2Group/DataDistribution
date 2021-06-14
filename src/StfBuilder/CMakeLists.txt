# @author Gvozden Neskovic
# @brief  cmake for StfBuilder

set(EXE_STFB_SOURCES
  StfBuilderInput
  StfBuilderDevice
  runStfBuilderDevice
)

add_library(StfBuilder_lib OBJECT ${EXE_STFB_SOURCES})
target_link_libraries(StfBuilder_lib base fmqtools common monitoring)

add_executable(StfBuilder)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(StfBuilder PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(StfBuilder
  PRIVATE
    StfBuilder_lib
    base fmqtools common monitoring
)

install(TARGETS StfBuilder RUNTIME DESTINATION bin)
