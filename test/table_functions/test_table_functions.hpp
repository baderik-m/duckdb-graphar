#pragma once
#include "duckdb_graphar_extension.hpp"
#include "functions/table/read_vertices.hpp"

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector.hpp>
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <graphar/graph_info.h>

using namespace duckdb;
using namespace graphar;

inline std::string TEST_DIR = "/Users/m.m.baderik/Projects/duckdb-graphar-mbaderik/test/data/";
inline std::string GRAPH_YML_PATH = TEST_DIR + "git/Git.yaml";
inline std::string LDBC_DIR = "/Users/m.m.baderik/Projects/duckdb-graphar-mbaderik/build/_deps/graphar-prefix/src/graphar/testing/ldbc_sample/parquet/";
inline std::string LDBC_GRAPH_YML_PATH = LDBC_DIR + "ldbc_sample.graph.yml";
inline std::string LDBC_GRAPH_WITH_FEATURE_YML_PATH = LDBC_DIR + "ldbc_sample_with_feature.graph.yml";

TableFunctionBindInput СreateMockBindInput(vector<Value> &inputs, named_parameter_map_t &named_parameters, vector<LogicalType> &input_table_types) {
    vector<std::string> input_table_names;
    TableFunction table_function;
    TableFunctionRef ref;

    return TableFunctionBindInput(inputs, named_parameters, input_table_types,
                                  input_table_names, nullptr, nullptr,
                                  table_function, ref);
}