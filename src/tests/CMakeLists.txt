# @author Gvozden Neskovic
# @brief  cmake for unit tests

# Unit test for FilePathUtils class

set(TEST_FILE_PATH_SOURCES
  test_FilePathUtils
  ../common/base/FilePathUtils
)
add_executable(test_FilePathUtils ${TEST_FILE_PATH_SOURCES})

target_include_directories(test_FilePathUtils
  PRIVATE
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/base
)
target_compile_definitions(test_FilePathUtils PRIVATE "BOOST_TEST_DYN_LINK=1")
target_link_libraries(test_FilePathUtils
  PUBLIC
  PRIVATE
    Boost::unit_test_framework
    Boost::filesystem
)

add_test(NAME FilePathUtils_test COMMAND test_FilePathUtils)


set(TEST_FMT_PATTERNS_SOURCES
  test_FmtPatterns
)
add_executable(test_FmtPatterns ${TEST_FMT_PATTERNS_SOURCES})
target_compile_definitions(test_FmtPatterns PRIVATE "BOOST_TEST_DYN_LINK=1")
target_link_libraries(test_FmtPatterns
  PUBLIC
  PRIVATE
    base
    Boost::unit_test_framework
    Boost::filesystem
)
add_test(NAME FmtPatterns_test COMMAND test_FmtPatterns)


set(TEST_BITMAP_SOURCES
  test_Bitmap
)
add_executable(test_Bitmap ${TEST_BITMAP_SOURCES})
target_compile_definitions(test_Bitmap PRIVATE "BOOST_TEST_DYN_LINK=1")
target_link_libraries(test_Bitmap
  PUBLIC
  PRIVATE
    base
    Boost::unit_test_framework
    Boost::filesystem
)
add_test(NAME Bitmap_test COMMAND test_Bitmap)
