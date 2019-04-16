# @author Gvozden Neskovic
# @brief  cmake for unit tests

# Unit test for FilePathUtils class

set(TEST_FILE_PATH_SOURCES
  test_FilePathUtils
  ../common/FilePathUtils
)
add_executable(test_FilePathUtils ${TEST_FILE_PATH_SOURCES})

target_include_directories(test_FilePathUtils
  PRIVATE
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
)
target_compile_definitions(test_FilePathUtils PRIVATE "BOOST_TEST_DYN_LINK=1")
target_link_libraries(test_FilePathUtils
  PUBLIC
  PRIVATE
    Boost::unit_test_framework
    Boost::filesystem
)

add_test(NAME FilePathUtils_test COMMAND test_FilePathUtils)

