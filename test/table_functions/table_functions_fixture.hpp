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

using PropertyValue = std::variant<int32_t, int64_t, float, double, std::string, bool>;

struct PropertySchema{
    std::string name;
    std::string data_type;
    bool is_nullable;
    bool is_primary;
};

struct EdgeData {
    int64_t src;
    int64_t dst;
    std::unordered_map<std::string, PropertyValue> properties;
};
struct EdgesSchema {
    std::string src_type;
    std::string type;
    std::string dst_type;
    graphar::IdType chunk_size;
    bool directed;
    std::vector<PropertySchema> properties;
    std::vector<EdgeData> edges;
    graphar::IdType num_vertices;
};
struct VertexData {
    int64_t ind;
    std::unordered_map<std::string, PropertyValue> properties;
};
struct VerticesSchema {
    std::string type;
    graphar::IdType chunk_size;
    std::vector<PropertySchema> properties;
    std::vector<VertexData> vertices;
};

std::string get_folder(std::string path){
    size_t found;
    found=path.find_last_of("/\\");
    return path.substr(0,found);
}

template <typename T>
requires(std::is_same_v<T, graphar::builder::Vertex> || std::is_same_v<T, graphar::builder::Edge>)
static void FillProperties(T& obj, const std::vector<PropertySchema>& propeties_schema, const std::unordered_map<std::string, PropertyValue>& properties_data) {
    for (auto ind_p = 0; ind_p < propeties_schema.size(); ++ind_p){
        const auto& prop_schema = propeties_schema[ind_p];
        auto it = properties_data.find(prop_schema.name);
        if (it == properties_data.end()) {
            throw std::runtime_error("Property not found in data: " + prop_schema.name);
        }
        const auto& prop_val = it->second;

        if (prop_schema.data_type == "int32") {
            if (std::holds_alternative<int32_t>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<int32_t>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected int32");
            }
        }
        else if (prop_schema.data_type == "int64") {
            if (std::holds_alternative<int64_t>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<int64_t>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected int64");
            }
        }
        else if (prop_schema.data_type == "float") {
            if (std::holds_alternative<float>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<float>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected float");
            }
        }
        else if (prop_schema.data_type == "double") {
            if (std::holds_alternative<double>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<double>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected double");
            }
        }
        else if (prop_schema.data_type == "string") {
            if (std::holds_alternative<std::string>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<std::string>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected string");
            }
        }
        else if (prop_schema.data_type == "bool") {
            if (std::holds_alternative<bool>(prop_val)) {
                obj.AddProperty(prop_schema.name, std::get<bool>(prop_val));
            } else {
                throw std::runtime_error("Type mismatch for property '" + prop_schema.name + "': expected bool");
            }
        }
        else {
            throw std::runtime_error("Unsupported data type: " + prop_schema.data_type);
        }
    } 
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
    std::string path_trial_feature_graph;
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
        const std::vector<VerticesSchema>&,
        const std::vector<EdgesSchema>& 
    );

    void EnsureTestDataDirectory();
};

template <typename FileTypeTag> 
size_t TableFunctionsFixture<FileTypeTag>::num_graph = 0;