#include "table_functions_fixture.hpp"

template<typename FileTypeTag>
std::string TableFunctionsFixture<FileTypeTag>::CreateTestGraph(
    const std::string& graph_name, 
    const std::vector<int64_t>& src_vertices,
    const std::vector<Edge>& edges,
    const std::vector<int64_t>& dst_vertices,
    const std::string& src_type,
    const std::string& edge_type, 
    const std::string& dst_type,
    bool directed,
    graphar::IdType src_chunksize,
    graphar::IdType dst_chunksize
){
    bool same_vertex = (src_type == dst_type);
    std::filesystem::path graph_folder = tmp_folder / GetFileTypeName() / (std::to_string(num_graph) + "_" + graph_name);
    ++num_graph;
    graph_folders.push_back(graph_folder);
    std::string output_path = graph_folder.string() + "/";
    std::string graph_path = output_path + graph_name + ".graph.yml";
    INFO("Creating graph: " + graph_name + " in " + graph_path);
    auto graph_info = graphar::GraphInfo::Load(graph_path).value_or(nullptr);
    if (graph_info != nullptr) {
        throw std::runtime_error("Graph already exists");
    }

    auto version = graphar::InfoVersion::Parse("gar/v1").value();

    // Vertex Info
    graphar::VertexInfoVector vertex_infos = {graphar::CreateVertexInfo(src_type, src_chunksize, {
        
    }, {}, "vertex/" + src_type + "/", version)};
    REQUIRE(!vertex_infos.front()->Dump().has_error());
    REQUIRE(vertex_infos.front()->Save(output_path + src_type + ".vertex.yml").ok());

    if (!same_vertex){
        vertex_infos.push_back(graphar::CreateVertexInfo(dst_type, dst_chunksize, {}, {}, "vertex/" + dst_type + "/", version));
        REQUIRE(!vertex_infos.back()->Dump().has_error());
        REQUIRE(vertex_infos.back()->Save(output_path + dst_type + ".vertex.yml").ok());
    } else {
        dst_chunksize = src_chunksize;
    }

    // Edge Info
    auto adjacent_types = {graphar::AdjListType::ordered_by_source, graphar::AdjListType::ordered_by_dest};
    graphar::AdjacentListVector adjacent_lists = {};
    for (const auto& adjacent_type : adjacent_types){ adjacent_lists.push_back(graphar::CreateAdjacentList(adjacent_type, GetFileType())); }

    auto edge_info = graphar::CreateEdgeInfo(
        src_type, edge_type, dst_type, src_chunksize * dst_chunksize, src_chunksize, dst_chunksize, directed,
        adjacent_lists,
        {}, "edge/" + src_type + "_" + edge_type + "_" + dst_type + "/", version);
    REQUIRE(!edge_info->Dump().has_error());
    REQUIRE(edge_info->Save(output_path + src_type + "_" + edge_type + "_" + dst_type + ".edge.yml").ok());

    // Graph Info
    graph_info = graphar::CreateGraphInfo(graph_name, vertex_infos, {edge_info}, {}, "./", version);
    REQUIRE(!graph_info->Dump().has_error());
    REQUIRE(graph_info->Save(graph_path).ok());

    // Vertices
    
    auto v_builder = graphar::builder::VerticesBuilder::Make(vertex_infos.front(), output_path, 0).value();
    for (const auto& v : src_vertices) {
        auto vertex = graphar::builder::Vertex(v);
        REQUIRE(v_builder->AddVertex(vertex).ok());
    }
    REQUIRE(v_builder->Dump().ok());

    if (!same_vertex){
        v_builder = graphar::builder::VerticesBuilder::Make(vertex_infos.back(), output_path, 0).value();
        for (const auto& v : dst_vertices) {
            auto vertex = graphar::builder::Vertex(v);
            REQUIRE(v_builder->AddVertex(vertex).ok());
        }
        REQUIRE(v_builder->Dump().ok());
    }


    // Edges
    auto num_vertices = src_vertices.size();
    if (!same_vertex){num_vertices += dst_vertices.size();}

    for (const auto& adjacent_type : adjacent_types){
        auto e_builder = graphar::builder::EdgesBuilder::Make(edge_info, output_path, adjacent_type, num_vertices).value();
        for (const auto& e : edges) {
            auto edge = graphar::builder::Edge(e.src, e.dst);
            REQUIRE(e_builder->AddEdge(edge).ok());
        }
        REQUIRE(e_builder->Dump().ok());
    }

    return graph_path;
}

template<typename FileTypeTag>
std::string TableFunctionsFixture<FileTypeTag>::CreateTestGraph(
    const std::string& graph_name, 
    const std::vector<int64_t>& vertices,
    const std::vector<Edge>& edges,
    const std::string& vertex_type, 
    const std::string& edge_type,
    bool directed,
    graphar::IdType vertex_chunksize
){
    return CreateTestGraph(graph_name, vertices, edges, vertices, vertex_type, edge_type, vertex_type, directed, vertex_chunksize, vertex_chunksize);
}


template<typename FileTypeTag>
TableFunctionsFixture<FileTypeTag>::TableFunctionsFixture(): db(nullptr), conn(db), tmp_folder(std::filesystem::temp_directory_path() / "duckdb_graphar/data/") {
    path_trial_graph = CreateTestGraph("tr", {1, 2, 3, 4, 5}, {
        {1, 2}, {1, 3}, {2, 3}, {2, 4}, {3, 4}, {3, 5}, {4, 5}
    }, "Person", "knows", false, 1024);
}

template<typename FileTypeTag>
TableFunctionsFixture<FileTypeTag>::~TableFunctionsFixture(){
    for (const auto& graph_folder : graph_folders){
        std::filesystem::remove_all(graph_folder);
    }
}
