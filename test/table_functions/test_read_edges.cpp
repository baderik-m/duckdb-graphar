#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include "duckdb_graphar_extension.hpp"
#include "functions/table/read_edges.hpp"

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector.hpp>

#include <graphar/graph_info.h>
// #include <graphar/edge_info.h>

using namespace duckdb;
using namespace graphar;

// Вспомогательная функция для создания тестового контекста
unique_ptr<ClientContext> CreateMockContext() {
    return ClientContext::Create();
}

// Вспомогательная функция для создания тестового GraphInfo
std::shared_ptr<GraphInfo> CreateTestGraphInfo() {
    // В реальности это должен быть полноценный GraphInfo с файлом
    auto graph_info = std::make_shared<GraphInfo>();
    // Здесь должен быть код для создания тестового графа
    return graph_info;
}

TEST_CASE("ReadEdges Bind function basic test", "[read_edges]") {
    auto context = CreateMockContext();
    TableFunctionBindInput input;
    vector<LogicalType> return_types;
    vector<string> names;

    // Подготовка входных данных
    input.inputs.push_back(Value("test.graphar"));
    input.named_parameters["src"] = Value("Person");
    input.named_parameters["dst"] = Value("City");
    input.named_parameters["type"] = Value("LivesIn");

    auto bind_data = ReadEdges::Bind(*context, input, return_types, names);

    REQUIRE(bind_data != nullptr);
    // Добавьте дополнительные проверки в соответствии с ожидаемым поведением
}

TEST_CASE("ReadEdges Bind function invalid_edge", "[read_edges]") {
    auto context = CreateMockContext();
    TableFunctionBindInput input;
    vector<LogicalType> return_types;
    vector<string> names;

    // Подготовка входных данных
    input.inputs.push_back(Value("test.graphar"));
    input.named_parameters["src"] = Value("Invalid");
    input.named_parameters["dst"] = Value("City");
    input.named_parameters["type"] = Value("LivesIn");

    // Ожидаем исключение, так как такого ребра нет
    REQUIRE_THROWS_AS(ReadEdges::Bind(*context, input, return_types, names), BinderException);
}

TEST_CASE("ReadEdges GetReader test", "[read_edges]") {
    // Подготовка тестовых данных
    auto graph_info = CreateTestGraphInfo();
    auto edge_info = graph_info->GetEdgeInfo("Person", "LivesIn", "City");
    
    unique_ptr<ReadBindData> bind_data = make_uniq<ReadBindData>();
    ReadEdges::SetBindData(graph_info, *edge_info, bind_data);

    ReadBaseGlobalTableFunctionState gstate;
    
    // Тестирование GetReader
    auto reader = ReadEdges::GetReader(gstate, *bind_data, 0, "", "", "");
    REQUIRE(reader != nullptr);
    
    // Тестирование с фильтром по src
    reader = ReadEdges::GetReader(gstate, *bind_data, 0, "123", SRC_GID_COLUMN, "int64");
    REQUIRE(reader != nullptr);
    
    // Тестирование с фильтром по dst
    reader = ReadEdges::GetReader(gstate, *bind_data, 0, "456", DST_GID_COLUMN, "int64");
    REQUIRE(reader != nullptr);
}

TEST_CASE("ReadEdges SetFilter test", "[read_edges]") {
    // Подготовка тестовых данных
    auto graph_info = CreateTestGraphInfo();
    auto edge_info = graph_info->GetEdgeInfo("Person", "LivesIn", "City");
    
    unique_ptr<ReadBindData> bind_data = make_uniq<ReadBindData>();
    ReadEdges::SetBindData(graph_info, *edge_info, bind_data);

    ReadBaseGlobalTableFunctionState gstate;
    
    std::string filter_value = "123";
    std::string filter_column = SRC_GID_COLUMN;
    std::string filter_type = "int64";
    
    // Тестирование установки фильтра
    ReadEdges::SetFilter(gstate, *bind_data, filter_value, filter_column, filter_type);
    
    // Проверка, что фильтр установлен
    REQUIRE(gstate.filter_range.first == 0);
    // Может потребовать дополнительных проверок в зависимости от реализации
}

TEST_CASE("ReadEdges GetFunction test", "[read_edges]") {
    TableFunction read_edges = ReadEdges::GetFunction();
    
    // Проверка базовых свойств функции
    REQUIRE(read_edges.name == "read_edges");
    REQUIRE(read_edges.parameters.size() == 1);
    REQUIRE(read_edges.named_parameters.size() == 3);
    REQUIRE(read_edges.filter_pushdown == true);
    REQUIRE(read_edges.projection_pushdown == true);
    
    // Проверка наличия всех необходимных параметров
    REQUIRE(read_edges.named_parameters.find("src") != read_edges.named_parameters.end());
    REQUIRE(read_edges.named_parameters.find("dst") != read_edges.named_parameters.end());
    REQUIRE(read_edges.named_parameters.find("type") != read_edges.named_parameters.end());
}

TEST_CASE("ReadEdges GetScanFunction test", "[read_edges]") {
    TableFunction scan_edges = ReadEdges::GetScanFunction();
    
    // Проверка базовых свойств сканирующей функции
    REQUIRE(scan_edges.name == "");
    REQUIRE(scan_edges.parameters.empty());
    REQUIRE(scan_edges.filter_pushdown == true);
    REQUIRE(scan_edges.projection_pushdown == true);
}