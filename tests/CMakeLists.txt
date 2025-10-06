log_stage("Declaring Catch2")
include(FetchContent)
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.5.2 # The stable version of catch2
)
FetchContent_MakeAvailable(Catch2)
get_property(catch_targets GLOBAL PROPERTY Catch2_IMPORTED_TARGETS)
log_done()

get_target_property(CATCH2_INCLUDE_DIRS Catch2::Catch2 INTERFACE_INCLUDE_DIRECTORIES)

# Unittests
# add_subdirectory(storage)
add_subdirectory(table_functions)
add_subdirectory(scalar_functions)


add_executable(unittest_graphar ${ALL_OBJECT_FILES})
target_include_directories(unittest_graphar
    PUBLIC
        ${EXTENSION_INCLUDES}
        ${CATCH2_INCLUDE_DIRS}
        ${EXTENSION_ROOT_DIR}/tests/fixture
)
add_dependencies(unittest_graphar graphar::graphar_shared)

if(APPLE)
    set(RPATH_ORIGIN "@loader_path")
else()
    set(RPATH_ORIGIN "\$ORIGIN") 
endif()

add_custom_command(TARGET unittest_graphar POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${ARROW_INSTALL_DIR}/lib/*${CMAKE_SHARED_LIBRARY_SUFFIX}"
        "$<TARGET_FILE_DIR:unittest_graphar>"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${GRAPHAR_INSTALL_DIR}/lib/libgraphar${CMAKE_SHARED_LIBRARY_SUFFIX}"
        "$<TARGET_FILE_DIR:unittest_graphar>"
    COMMENT "Copying dependencies to output directory"
)

set_property(TARGET unittest_graphar PROPERTY INSTALL_RPATH "${RPATH_ORIGIN}")
set_property(TARGET unittest_graphar PROPERTY SKIP_BUILD_RPATH OFF)
set_property(TARGET unittest_graphar PROPERTY BUILD_WITH_INSTALL_RPATH ON)

message("${ALL_OBJECT_FILES}")
target_link_libraries(unittest_graphar ${DUCKDB_EXTENSION_NAME})

# Link against DuckDB and catch2.
# Catch2::Catch2WithMain provides the main() function for the test runner.
target_link_libraries(unittest_graphar duckdb_static arrow::arrow_shared graphar::graphar_shared Catch2::Catch2WithMain)

target_include_directories(unittest_graphar PRIVATE ${DUCKDB_INCLUDES} ${EXTENSION_INCLUDES})

# Integrate with ctest for easy test execution.
add_test(NAME ${DUCKDB_EXTENSION_NAME}_unittest COMMAND unittest_graphar)
message(STATUS "Unittests done")
