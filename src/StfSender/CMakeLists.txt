# @author Gvozden Neskovic
# @brief  cmake for SubTimeFrameSender

set(EXE_STFS_SOURCES
  SubTimeFrameSenderDevice
  SubTimeFrameSenderOutput
  runSubTimeFrameSenderDevice
)

add_executable(StfSender ${EXE_STFS_SOURCES} $<TARGET_OBJECTS:common>)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(StfSender PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_include_directories(StfSender
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
    ${MS_GSL_INCLUDE_DIR}
)

target_link_libraries(StfSender
  PUBLIC
  PRIVATE
    FairMQ::FairMQ
    AliceO2::Headers
    ${Monitoring_LIBRARIES}
    ROOT::Hist
    ROOT::Gpad
)

install(TARGETS StfSender RUNTIME DESTINATION bin)
