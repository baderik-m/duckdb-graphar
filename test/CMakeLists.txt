message(STATUS "Building unit tests for extension")
 
# Fetch the catch2 testing framework using CMake's FetchContent
include(FetchContent)
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.5.2 # The stable version of catch2
)
FetchContent_MakeAvailable(Catch2)
get_property(catch_targets GLOBAL PROPERTY Catch2_IMPORTED_TARGETS)
message(STATUS "Imported Catch2 targets: ${catch_targets}")
# Add the test runner executable, which includes your test files.
# unittest_main.cpp will contain your test cases.
add_executable(unittest "${EXTENSION_ROOT_DIR}/test/unittests/bfs_tests.cpp")

# Link the test runner against your extension's code.
# This makes your Storage Manager's classes and functions available to the tests.
target_link_libraries(unittest PRIVATE ${DUCKDB_EXTENSION_NAME})

# Link against DuckDB and catch2.
# Catch2::Catch2WithMain provides the main() function for the test runner.
target_link_libraries(unittest PRIVATE duckdb graphar::graphar_shared Catch2::Catch2WithMain)

# Ensure the test runner can find DuckDB's header files.
target_include_directories(unittest PRIVATE ${DUCKDB_INCLUDES})
target_include_directories(unittest PRIVATE ${EXTENSION_INCLUDES})

# Integrate with ctest for easy test execution.
add_test(NAME ${DUCKDB_EXTENSION_NAME}_unittest COMMAND unittest)
message(STATUS "Unittests done")