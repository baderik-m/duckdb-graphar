include(FetchContent)
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.5.2 # The stable version of catch2
)
FetchContent_MakeAvailable(Catch2)
get_property(catch_targets GLOBAL PROPERTY Catch2_IMPORTED_TARGETS)
message(STATUS "Imported Catch2 targets: ${catch_targets}")

# include_directories(${DUCKDB_INCLUDES})
include_directories(${EXTENSION_INCLUDES})
get_target_property(CATCH2_INCLUDE_DIRS Catch2::Catch2 INTERFACE_INCLUDE_DIRECTORIES)
include_directories(${CATCH2_INCLUDE_DIRS})
# add_subdirectory(scalar_functions)

# unittest
# add_subdirectory(storage)
add_subdirectory(table_functions)

add_executable(unittest_graphar ${ALL_OBJECT_FILES})
message("${ALL_OBJECT_FILES}")
target_link_libraries(unittest_graphar ${DUCKDB_EXTENSION_NAME})

# Link against DuckDB and catch2.
# Catch2::Catch2WithMain provides the main() function for the test runner.
target_link_libraries(unittest_graphar duckdb arrow::arrow_shared graphar::graphar_shared Catch2::Catch2WithMain)

target_include_directories(unittest_graphar PRIVATE ${DUCKDB_INCLUDES} ${EXTENSION_INCLUDES})

# Integrate with ctest for easy test execution.
add_test(NAME ${DUCKDB_EXTENSION_NAME}_unittest COMMAND unittest_graphar)
message(STATUS "Unittests done")
