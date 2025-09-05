#pragma once
#include <catch2/catch_test_macros.hpp>

#include "duckdb.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <string>

#include <graphar/api/high_level_writer.h>
#include <graphar/graph_info.h>
#include <graphar/status.h>

#include "functions/table/read_edges.hpp"
#include "functions/table/read_vertices.hpp"

#include <cassert>
#include <iostream>
#include <sstream>
#include <variant>
#include <filesystem>

struct Edge {
    int64_t src;
    int64_t dst;
};

struct PropetrySchema{
    std::string name;
    std::string data_type;
    bool is_nullable;
    bool is_primary;
};
struct EdgesSchema {
    std::string src_type;
    std::string type;
    std::string dst_type;
    graphar::IdType chunk_size;
    bool directed;
    std::vector<PropetrySchema> properties;
    std::vector<Edge> values;
    graphar::IdType num_vertices;
};
struct VerteciesSchema {
    std::string type;
    graphar::IdType chunk_size;
    std::vector<PropetrySchema> properties;
    std::vector<std::vector<int64_t>> values;
};

std::string get_folder(std::string path){
    size_t found;
    found=path.find_last_of("/\\");
    return path.substr(0,found);
}

struct FileTypeParquet {};
struct FileTypeCsv {};
struct FileTypeOrc {};
struct FileTypeJson {};

template <typename FileTypeTag> 
class TableFunctionsFixture {
protected:
    duckdb::DuckDB db;
    duckdb::Connection conn;
    std::string path_trial_graph;
    static duckdb::TableFunctionBindInput СreateMockBindInput(duckdb::vector<duckdb::Value> &inputs, duckdb::named_parameter_map_t &named_parameters, duckdb::vector<duckdb::LogicalType> &input_table_types) {
        duckdb::vector<std::string> input_table_names;
        duckdb::TableFunction table_function;
        duckdb::TableFunctionRef ref;

        return duckdb::TableFunctionBindInput(inputs, named_parameters, input_table_types,
                                    input_table_names, nullptr, nullptr,
                                    table_function, ref);
    }
public:
    TableFunctionsFixture();
    ~TableFunctionsFixture();    
    
private:
    static size_t num_graph;
    constexpr static std::string data_dir = "tmp/graphs/";
    std::filesystem::path tmp_folder;
    std::vector<std::filesystem::path> graph_folders;
    static constexpr graphar::FileType GetFileType() {
        if constexpr (std::is_same_v<FileTypeTag, FileTypeParquet>) return graphar::FileType::PARQUET;
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeCsv>) return graphar::FileType::CSV;
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeOrc>) return graphar::FileType::ORC;
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeJson>) return graphar::FileType::JSON;
        else throw std::logic_error("Unsupported file type tag");
    };
    static constexpr std::string GetFileTypeName() {
        if constexpr (std::is_same_v<FileTypeTag, FileTypeParquet>) return "parquet";
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeCsv>) return "csv";
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeOrc>) return "orc";
        else if constexpr (std::is_same_v<FileTypeTag, FileTypeJson>) return "json";
        else throw std::logic_error("Unsupported file type tag");
    };

    std::string CreateTestGraph(
        const std::string&, 
        const std::vector<VerteciesSchema>&,
        const std::vector<EdgesSchema>& 
    );

    void EnsureTestDataDirectory();
};

template <typename FileTypeTag> 
size_t TableFunctionsFixture<FileTypeTag>::num_graph = 0;