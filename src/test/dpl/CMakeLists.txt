# @author Gvozden Neskovic
# @brief  DataDistribution and DPL integration

set(EXE_DPL_TEST1_SOURCES
  test_StfBuilderDplAdapter
)

add_executable(test_StfBuilderDplAdapter ${EXE_DPL_TEST1_SOURCES} $<TARGET_OBJECTS:common>)

target_include_directories(test_StfBuilderDplAdapter
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../common/include
)

target_link_libraries(test_StfBuilderDplAdapter
  PUBLIC
  PRIVATE
    FairMQ::FairMQ
    AliceO2::Headers
    AliceO2::Framework
    ROOT::Gui
)

install(TARGETS test_StfBuilderDplAdapter RUNTIME DESTINATION bin)
