# Configuration for the DuckDB 'duckdb_graphar' extension.
#

duckdb_extension_load(duckdb_graphar
    SOURCE_DIR "${EXTENSION_ROOT_DIR}/config"
    INCLUDE_DIR "${EXTENSION_ROOT_DIR}/include"
    LINKED_LIBS "arrow;graphar"
<<<<<<< HEAD
    TEST_DIR "${EXTENSION_ROOT_DIR}/test/sql"
=======
>>>>>>> main
    LOAD_TESTS
)
