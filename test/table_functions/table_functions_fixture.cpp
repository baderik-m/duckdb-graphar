#include "table_functions_fixture.hpp"

template<typename FileTypeTag>
std::string TableFunctionsFixture<FileTypeTag>::CreateTestGraph(
    const std::string& graph_name, 
    const std::vector<VerteciesSchema>& vertices,
    const std::vector<EdgesSchema>& edges
){
    std::filesystem::path graph_folder = tmp_folder / GetFileTypeName() / (std::to_string(num_graph) + "_" + graph_name);
    ++num_graph;
    graph_folders.push_back(graph_folder);
    std::string output_path = graph_folder.string() + "/";
    std::string graph_path = output_path + graph_name + ".graph.yaml";
    INFO("Creating graph: " + graph_name + " in " + graph_path);
    auto graph_info = graphar::GraphInfo::Load(graph_path).value_or(nullptr);
    if (graph_info != nullptr) {
        throw std::runtime_error("Graph already exists");
    }

    auto version = graphar::InfoVersion::Parse("gar/v1").value();

    // Vertex Info
    std::unordered_map<std::string, const VerteciesSchema*> type_schema;
    graphar::VertexInfoVector vertex_infos = {};
    for (const auto& vertex_schema: vertices){
        std::vector<graphar::Property> properties;
        for (const auto& prop_schema: vertex_schema.properties){
            properties.push_back(graphar::Property(
                prop_schema.name, graphar::DataType::TypeNameToDataType(prop_schema.data_type),
                prop_schema.is_primary, prop_schema.is_nullable)
            );
        }
        // graphar::PropertyGroup(properties, GetFileType());
        const graphar::PropertyGroupVector pgs = {
            std::make_shared<graphar::PropertyGroup>(properties, GetFileType(), "")
        };
        vertex_infos.push_back(graphar::CreateVertexInfo(
            vertex_schema.type, vertex_schema.chunk_size,
            pgs, {}, 
            "vertex/" + vertex_schema.type + "/", version));
        REQUIRE(!vertex_infos.back()->Dump().has_error());
        REQUIRE(vertex_infos.back()->Save(output_path + vertex_schema.type + ".vertex.yaml").ok());
        type_schema[vertex_schema.type] = &vertex_schema;
    }


    // Edge Info
    graphar::EdgeInfoVector edges_infos = {};

    const auto adjacent_types = {graphar::AdjListType::ordered_by_source, graphar::AdjListType::ordered_by_dest};
    graphar::AdjacentListVector adjacent_lists = {};
    for (const auto& adjacent_type : adjacent_types){ 
        adjacent_lists.push_back(graphar::CreateAdjacentList(adjacent_type, GetFileType())); 
    }

    for (const auto& edge_schema: edges){
        graphar::IdType src_chunk_size = type_schema[edge_schema.src_type]->chunk_size;
        graphar::IdType dst_chunk_size = type_schema[edge_schema.dst_type]->chunk_size;
        auto edge_chunk_size = (edge_schema.chunk_size > 0) ? edge_schema.chunk_size : src_chunk_size * dst_chunk_size;
        edges_infos.push_back(graphar::CreateEdgeInfo(
            edge_schema.src_type, edge_schema.type, edge_schema.dst_type, 
            edge_chunk_size, src_chunk_size, dst_chunk_size, edge_schema.directed,
            adjacent_lists,
            {}, "edge/" + edge_schema.src_type + "_" + edge_schema.type + "_" + edge_schema.dst_type + "/", version));
        REQUIRE(!edges_infos.back()->Dump().has_error());
        REQUIRE(edges_infos.back()->Save(output_path + edge_schema.src_type + "_" + edge_schema.type + "_" + edge_schema.dst_type + ".edge.yaml").ok());
    }

    // Graph Info
    graph_info = graphar::CreateGraphInfo(graph_name, vertex_infos, edges_infos, {}, "./", version);
    REQUIRE(!graph_info->Dump().has_error());
    REQUIRE(graph_info->Save(graph_path).ok());


    // Vertices
    REQUIRE(vertices.size() == 1);
    for (auto ind_v = 0; ind_v < vertices.size(); ++ind_v){
        const auto& vertex_schema = vertices[ind_v];
        auto v_builder = graphar::builder::VerticesBuilder::Make(vertex_infos[ind_v], output_path, 0).value();
        for (const auto& v : vertex_schema.values) {
            auto vertex = graphar::builder::Vertex(v[0]);

            for (auto ind_p = 0; ind_p < vertex_schema.properties.size(); ++ind_p){
                const auto& prop = vertex_schema.properties[ind_p];
                    if (prop.data_type == "int32") {
                    vertex.AddProperty(prop.name, static_cast<int32_t>(v[ind_p + 1]));
                } else if (prop.data_type == "int64") {
                    vertex.AddProperty(prop.name, static_cast<int64_t>(v[ind_p + 1]));
                } else {
                    // Пока поддерживаем только int
                    throw std::runtime_error("Unsupported data type: " + prop.data_type);
                }
            }
            REQUIRE(v_builder->AddVertex(vertex).ok());
        }
        REQUIRE(v_builder->Dump().ok());
    }

    // Edges
    REQUIRE(edges.size() == 1);

    for (auto ind_e = 0; ind_e < edges.size(); ++ind_e){
        const auto& edge_schema = edges[ind_e];
        auto num_vertices = edge_schema.num_vertices;
        for (const auto& adjacent_type : adjacent_types){
            auto e_builder = graphar::builder::EdgesBuilder::Make(edges_infos[ind_e], output_path, adjacent_type, num_vertices).value();
            for (const auto& e : edge_schema.values) {
                auto edge = graphar::builder::Edge(e.src, e.dst);
                // for (auto ind_p = 0; ind_p < edge_schema.properties.size(); ++ind_p){
                //     edge.AddProperty(edge_schema.properties[ind_p].name, e.properties[ind_p]);
                // }
                REQUIRE(e_builder->AddEdge(edge).ok());
            }
            REQUIRE(e_builder->Dump().ok());
        }
    }

    return graph_path;
}


template<typename FileTypeTag>
TableFunctionsFixture<FileTypeTag>::TableFunctionsFixture(): db(nullptr), conn(db), tmp_folder(std::filesystem::temp_directory_path() / "duckdb_graphar/data/") {
    path_trial_graph = CreateTestGraph(
        "tr", 
        {
            VerteciesSchema(
                "Person", 1024, 
                {PropetrySchema("hash_phone_no", "int32", false, true)}, 
                {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}
            )
        }, 
        {
            EdgesSchema(
                "Person", "knows", "Person", 0, false, 
                {PropetrySchema("creationDate", "string", false, false)}, 
                {{1, 2}, {1, 3}, {2, 3}, {2, 4}, {3, 4}, {3, 5}, {4, 5}}
            )
        }
    );
}

template<typename FileTypeTag>
TableFunctionsFixture<FileTypeTag>::~TableFunctionsFixture(){
    for (const auto& graph_folder : graph_folders){
        std::filesystem::remove_all(graph_folder);
    }
}
