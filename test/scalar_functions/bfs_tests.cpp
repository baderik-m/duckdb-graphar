#include <memory>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <graphar/api/high_level_writer.h>
#include <graphar/graph_info.h>
#include <graphar/status.h>

#include "duckdb_graphar_extension.hpp"
#include "functions/scalar/bfs.hpp"

using namespace duckdb;
struct Edge {
    int64_t src;
    int64_t dst;
};

#define ADJLIST_TYPE graphar::AdjListType::ordered_by_source

std::string createAndSaveGraphAr(const std::string& graph_name, const std::vector<int64_t>& vertices,
                          const std::vector<Edge>& edges, const std::string& output_path = "test/data/",
                          int64_t vertex_chunk_size = 1024, int64_t edge_chunk_size = 1024 * 1024
                        ) {
    std::string save_path = output_path + "/" + graph_name + "/";
    auto version = graphar::InfoVersion::Parse("gar/v1").value();

    std::string type = "Person", vertex_prefix = "vertex/Person/";

    auto vertex_info = graphar::CreateVertexInfo(type, vertex_chunk_size, {}, {},
                                                vertex_prefix, version);

    assert(!vertex_info->Dump().has_error());
    assert(vertex_info->Save(save_path + "Person.vertex.yml").ok());

    /*------------------construct edge info------------------*/
    std::string src_type = "Person", edge_type = "knows", dst_type = "Person",
                edge_prefix = "edge/Person_knows_Person/";
    bool directed = false;

    auto adjacent_lists = {
        graphar::CreateAdjacentList(ADJLIST_TYPE, graphar::FileType::PARQUET)};

    auto edge_info = graphar::CreateEdgeInfo(
        src_type, edge_type, dst_type, edge_chunk_size, vertex_chunk_size,
        vertex_chunk_size, directed, adjacent_lists, {}, edge_prefix, version);

    assert(!edge_info->Dump().has_error());
    assert(edge_info->Save(save_path + "Person_knows_Person.edge.yml").ok());

    /*------------------construct graph info------------------*/
    auto graph_info = graphar::CreateGraphInfo(
        graph_name, {vertex_info}, {edge_info}, {}, save_path, version);

    assert(!graph_info->Dump().has_error());
    std::string graph_path = save_path + graph_name + ".graph.yml";
    assert(graph_info->Save(graph_path).ok());

    /*------------------construct vertices------------------*/
    graphar::IdType start_index = 0;
    auto v_builder = graphar::builder::VerticesBuilder::Make(
                        vertex_info, save_path, start_index)
                        .value();

    for (int i = 0; i < vertices.size(); i++) {
        graphar::builder::Vertex v;
        assert(v_builder->AddVertex(v).ok());
    }

    assert(v_builder->GetNum() == vertices.size());
    
    assert(v_builder->Dump().ok());

    v_builder->Clear();

    /*------------------construct edges------------------*/
    auto e_builder = graphar::builder::EdgesBuilder::Make(
                        edge_info, save_path, ADJLIST_TYPE, vertices.size())
                        .value();

    for (const Edge& edge : edges){
        graphar::builder::Edge e(edge.src, edge.dst);
        assert(e_builder->AddEdge(e).ok());
    }
    assert(e_builder->Dump().ok());

    e_builder->Clear();
    return graph_path;
}


std::string GetTrialGraph() {
    std::string path = "test/data/";
    std::string graph_name = "trial_graph";
    std::string graph_path = path+graph_name+"/"+graph_name+".graph.yml";

    auto graph_info = graphar::GraphInfo::Load(graph_path).value_or(nullptr);
    if (graph_info != nullptr){ return graph_path; }
    return createAndSaveGraphAr(graph_name, {0, 1, 2, 3, 4}, {{0,1}, {1,3}, {2, 0}, {2, 1}}, path);

}

std::string GetLongGraph() {
    std::string path = "test/data/";
    std::string graph_name = "long_graph";
    std::string graph_path = path+graph_name+"/"+graph_name+".graph.yml";
    
    auto graph_info = graphar::GraphInfo::Load(graph_path).value_or(nullptr);
    if (graph_info != nullptr){ return path; }
    
    std::vector<int64_t> vertices(530);
    std::vector<Edge> edges(556);
    vertices[0] = 0;
    vertices[539]= 539;
    for (int i = 1; i < 30; ++i) {
        edges[2*(i-1)] = {0, i };
        edges[2*(i-1)+1] = {30, i};
        vertices[i] = i;
    }
    for (int i = 31; i < 528; ++i) {
        edges[27 + i] = {i-1, i };
        vertices[i] = i;
    }
    return createAndSaveGraphAr(graph_name, vertices, edges, path);
}

TEST_CASE("BFS Basic Functionality", "[bfs]") {
    DuckDB db(nullptr);
    Connection con(db);
    
    Bfs::Register(*db.instance);
    
    std::string graph_path = GetTrialGraph();
    
    SECTION("BFS Length - Direct Connection") {
        auto result = con.Query("SELECT bfs_length(0, 1, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }
    
    SECTION("BFS Length - Two Hops") {
        auto result = con.Query("SELECT bfs_length(0, 3, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 2);
    }
    
    SECTION("BFS Length - No Path") {
        auto result = con.Query("SELECT bfs_length(4, 0, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == -1);
    }
    
    SECTION("BFS Exists - Direct Connection") {
        auto result = con.Query("SELECT bfs_exist(0, 1, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<bool>());
    }
    
    SECTION("BFS Exists - No Path") {
        auto result = con.Query("SELECT bfs_exist(4, 0, '" + graph_path + "')");
        
        REQUIRE(!result->GetValue(0, 0).GetValue<bool>());
    }
}

TEST_CASE("BFS Performance", "[bfs][stress]") {
    DuckDB db(nullptr);
    Connection con(db);
    
    Bfs::Register(*db.instance);
    
    std::string graph_path = GetLongGraph();
    
    SECTION("BFS Long Path Stress Test (500 hops)") {
        std::string long_graph_path = "test_long_graph";
        
        BENCHMARK("BFS Long Path") {
            auto result = con.Query("SELECT bfs_length(0, 527, '" + long_graph_path + "')");
            return (result->GetValue(0, 0).GetValue<int64_t>() == 500);
        };

        BENCHMARK("BFS No Path") {
            auto result = con.Query("SELECT bfs_length(0, 528, '" + long_graph_path + "')");
            return (result->GetValue(0, 0).GetValue<int64_t>()==-1);
        };
    }
}