#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>
#include <iostream>

#include "table_functions_fixture.hpp"
#include "functions/table/read_vertices.hpp"

using namespace duckdb;
using namespace graphar;


TEST_CASE("ReadVertices GetFunction basic test", "[read_vertices]") {
    TableFunction read_vertices = ReadVertices::GetFunction();

    REQUIRE(read_vertices.name == "read_vertices");
    REQUIRE(read_vertices.arguments.size() == 1);
    REQUIRE(read_vertices.named_parameters.size() == 1);
    REQUIRE(read_vertices.filter_pushdown == true);
    REQUIRE(read_vertices.projection_pushdown == true);

    REQUIRE(read_vertices.named_parameters.find("type") != read_vertices.named_parameters.end());
}

/*
TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "test_load_yml", "[tmp]", FileTypeParquet, FileTypeCsv){
    auto res = graphar::GraphInfo::Load(TableFunctionsFixture<TestType>::path_trial_graph);
    if (res.has_error()){
        const Status& status = res.error();
        std::string error_message = status.message();
        REQUIRE("" == error_message);
    }
}
*/
TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadVertices Bind function basic test", "[read_vertices]", FileTypeParquet, FileTypeCsv) {
    TableFunction read_vertices = ReadVertices::GetFunction();
    
    vector<Value> inputs({Value(TableFunctionsFixture<TestType>::path_trial_graph)});
    named_parameter_map_t named_parameters({{"type", Value("Person")}});
    vector<LogicalType> input_table_types({});
    auto input = TableFunctionsFixture<TestType>::СreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    INFO("Start bind");
    auto bind_data = read_vertices.bind(*TableFunctionsFixture<TestType>::conn.context, input, return_types, names);
    INFO("Finish bind");

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::INTEGER}));
    REQUIRE(names == vector<std::string>({GID_COLUMN_INTERNAL, "hash_phone_no"}));
}
/*
TEST_CASE("ReadVertices Bind function invalid_vertex", "[read_vertices]") {
    TableFunction read_vertices = ReadVertices::GetFunction();

    INFO("Start mocking");
    DuckDB db(nullptr);
    Connection conn(db);

    vector<Value> inputs({Value(GRAPH_YML_PATH)});
    named_parameter_map_t named_parameters({{"type", Value("InvalidVertex")}});
    vector<LogicalType> input_table_types({});
    auto input = СreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    REQUIRE_THROWS_AS(read_vertices.bind(*conn.context, input, return_types, names), BinderException);
}

TEST_CASE("ReadVertices Bind function vertex with basic properties", "[read_vertices]") {
    TableFunction read_vertices = ReadVertices::GetFunction();

    INFO("Start mocking");
    DuckDB db(nullptr);
    Connection conn(db);

    vector<Value> inputs({Value(LDBC_GRAPH_YML_PATH)});
    named_parameter_map_t named_parameters({{"type", Value("person")}});
    vector<LogicalType> input_table_types({});
    auto input = СreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    INFO("Start bind");
    auto bind_data = read_vertices.bind(*conn.context, input, return_types, names);
    INFO("Finish bind");

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
    REQUIRE(names == vector<std::string>({GID_COLUMN_INTERNAL, "id", "firstName", "lastName", "gender"}));
}

TEST_CASE("ReadVertices Bind function vertex with array properties", "[read_vertices]") {
    TableFunction read_vertices = ReadVertices::GetFunction();

    INFO("Start mocking");
    DuckDB db(nullptr);
    Connection conn(db);

    vector<Value> inputs({Value(LDBC_GRAPH_WITH_FEATURE_YML_PATH)});
    named_parameter_map_t named_parameters({{"type", Value("person")}});
    vector<LogicalType> input_table_types({});
    auto input = СreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    INFO("Start bind");
    REQUIRE_THROWS_AS(read_vertices.bind(*conn.context, input, return_types, names), NotImplementedException);
    INFO("Finish bind");
}

TEST_CASE("ReadVertices GetScanFunction basic test", "[read_vertices]") {
    TableFunction scan_vertices = ReadVertices::GetScanFunction();

    REQUIRE(scan_vertices.name == "");
    REQUIRE(scan_vertices.arguments.empty());
    REQUIRE(scan_vertices.filter_pushdown == true);
    REQUIRE(scan_vertices.projection_pushdown == true);
}

TEST_CASE("ReadVertices GetReader test", "[read_vertices]") {
    INFO("Creating db");
    DuckDB db(nullptr);
    Connection conn(db);

    INFO("Start mocking");
    vector<Value> inputs({Value(GRAPH_YML_PATH)});
    named_parameter_map_t named_parameters({{"type", Value("Person")}});
    vector<LogicalType> input_table_types;
    auto input = СreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    auto bind_data_uniq = ReadVertices::GetFunction().bind(*conn.context, input, return_types, names);
    auto& bind_data = bind_data_uniq->Cast<ReadBindData>();

    ReadBaseGlobalTableFunctionState gstate;

    SECTION("Тест без фильтра"){
        auto reader = ReadVertices::GetReader(gstate, bind_data, 0, "", "", "");
        REQUIRE(reader != nullptr);

        auto result = GetChunk(*reader);
        REQUIRE(!result.has_error());
        auto table = result.value();
        REQUIRE(table->num_rows() == 37700);  // Количество вершин типа "Person" в SNAP GitHub
        REQUIRE(table->num_columns() == 2); // GID + 1 свойство
    }

    SECTION("Тест с фильтром по GID"){
        auto reader = ReadVertices::GetReader(gstate, bind_data, 0, "0", GID_COLUMN_INTERNAL, "int64");
        REQUIRE(reader != nullptr);

        auto result = GetChunk(*reader);
        REQUIRE(!result.has_error());
        auto table = result.value();
        REQUIRE(table->num_rows() == 1);
    }
}

TEST_CASE("ReadVertices Execute basic test", "[read_vertices]") {
    INFO("Creating db");
    DuckDB db(nullptr);
    Connection conn(db);
    INFO("Start mocking");
    vector<Value> inputs({Value(LDBC_GRAPH_YML_PATH)});
    named_parameter_map_t named_parameters({{"type", Value("person")}});
    vector<LogicalType> input_table_types;
    auto input = СreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    auto read_vertices =  ReadVertices::GetFunction();
    auto bind_data_uniq = read_vertices.bind(*conn.context, input, return_types, names);
    
    auto& bind_data = bind_data_uniq->Cast<ReadBindData>();

    vector<column_t> columns_idx(return_types.size());
    for (idx_t i = 0; i < return_types.size(); i++) {
        columns_idx[i] = i;
    }

    TableFunctionInitInput init_input(bind_data_uniq.get(), columns_idx, vector<idx_t>(), nullptr);
    auto global_state = read_vertices.init_global(*conn.context, init_input);
    INFO("Created global state");

    DataChunk output;
    output.Initialize(*conn.context, return_types);
    INFO("Created output");

    auto funcInput = TableFunctionInput{*bind_data_uniq, nullptr, global_state};
    INFO("Created funcInput");

    ReadVertices::Execute(*conn.context, funcInput, output);
    
    REQUIRE(output.size() == 100); // Размер одного chunk
    REQUIRE(output.ColumnCount() == return_types.size());
}
*/