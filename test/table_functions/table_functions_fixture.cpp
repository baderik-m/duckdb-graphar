#include "table_functions_fixture.hpp"


template<typename FileTypeTag>
std::string TableFunctionsFixture<FileTypeTag>::CreateTestGraph(
    const std::string& graph_name, 
    const std::vector<VerticesSchema>& vertices_list,
    const std::vector<EdgesSchema>& edges_list
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
    std::unordered_map<std::string, const VerticesSchema*> type_schema;
    graphar::VertexInfoVector vertex_infos = {};
    for (const auto& vertices_schema: vertices_list){
        std::vector<graphar::Property> properties;
        for (const auto& prop_schema: vertices_schema.properties){
            properties.push_back(graphar::Property(
                prop_schema.name, graphar::DataType::TypeNameToDataType(prop_schema.data_type),
                prop_schema.is_primary, prop_schema.is_nullable)
            );
        }
        const graphar::PropertyGroupVector pgs = {
            std::make_shared<graphar::PropertyGroup>(properties, GetFileType(), "")
        };
        vertex_infos.push_back(graphar::CreateVertexInfo(
            vertices_schema.type, vertices_schema.chunk_size,
            pgs, {}, 
            "vertex/" + vertices_schema.type + "/", version));
        REQUIRE(!vertex_infos.back()->Dump().has_error());
        REQUIRE(vertex_infos.back()->Save(output_path + vertices_schema.type + ".vertex.yaml").ok());
        type_schema[vertices_schema.type] = &vertices_schema;
    }

    // Edge Info
    graphar::EdgeInfoVector edges_infos = {};

    const auto adjacent_types = {graphar::AdjListType::ordered_by_source, graphar::AdjListType::ordered_by_dest};
    graphar::AdjacentListVector adjacent_lists = {};
    for (const auto& adjacent_type : adjacent_types){ 
        adjacent_lists.push_back(graphar::CreateAdjacentList(adjacent_type, GetFileType())); 
    }

    for (const auto& edges_schema: edges_list){
        graphar::IdType src_chunk_size = type_schema[edges_schema.src_type]->chunk_size;
        graphar::IdType dst_chunk_size = type_schema[edges_schema.dst_type]->chunk_size;
        auto edge_chunk_size = (edges_schema.chunk_size > 0) ? edges_schema.chunk_size : src_chunk_size * dst_chunk_size;
        edges_infos.push_back(graphar::CreateEdgeInfo(
            edges_schema.src_type, edges_schema.type, edges_schema.dst_type, 
            edge_chunk_size, src_chunk_size, dst_chunk_size, edges_schema.directed,
            adjacent_lists,
            {}, "edge/" + edges_schema.src_type + "_" + edges_schema.type + "_" + edges_schema.dst_type + "/", version));
        REQUIRE(!edges_infos.back()->Dump().has_error());
        REQUIRE(edges_infos.back()->Save(output_path + edges_schema.src_type + "_" + edges_schema.type + "_" + edges_schema.dst_type + ".edge.yaml").ok());
    }

    // Graph Info
    graph_info = graphar::CreateGraphInfo(graph_name, vertex_infos, edges_infos, {}, "./", version);
    REQUIRE(!graph_info->Dump().has_error());
    REQUIRE(graph_info->Save(graph_path).ok());


    // vertices_list
    for (auto ind_v = 0; ind_v < vertices_list.size(); ++ind_v){
        const auto& vertices_schema = vertices_list[ind_v];
        auto v_builder = graphar::builder::VerticesBuilder::Make(vertex_infos[ind_v], output_path, 0).value();
        for (const auto& vertex_data : vertices_schema.vertices) {
            auto vertex = graphar::builder::Vertex(vertex_data.ind);
            FillProperties<graphar::builder::Vertex>(vertex, vertices_schema.properties, vertex_data.properties);

            REQUIRE(v_builder->AddVertex(vertex).ok());
        }
        REQUIRE(v_builder->Dump().ok());
    }

    // edges_list
    for (auto ind_e = 0; ind_e < edges_list.size(); ++ind_e){
        const auto& edges_schema = edges_list[ind_e];
        auto num_vertices = edges_schema.num_vertices;
        for (const auto& adjacent_type : adjacent_types){
            auto e_builder = graphar::builder::EdgesBuilder::Make(edges_infos[ind_e], output_path, adjacent_type, num_vertices).value();
            for (const auto& edge_data : edges_schema.edges) {
                auto edge = graphar::builder::Edge(edge_data.src, edge_data.dst);
                FillProperties<graphar::builder::Edge>(edge, edges_schema.properties, edge_data.properties);
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
        "trial", 
        {
            VerticesSchema(
                "Person", 1024, 
                {PropertySchema("hash_phone_no", "int32", false, true)}, 
                {
                    {1, {{"hash_phone_no", int32_t{10}}}}, 
                    {2, {{"hash_phone_no", int32_t{20}}}}, 
                    {3, {{"hash_phone_no", int32_t{30}}}}, 
                    {4, {{"hash_phone_no", int32_t{40}}}}, 
                    {5, {{"hash_phone_no", int32_t{50}}}}
                }
            )
        }, 
        {
            EdgesSchema(
                "Person", "knows", "Person", 0, false, 
                {}, 
                {
                    {1, 2}, 
                    {1, 3}, 
                    {2, 3}, 
                    {2, 4}, 
                    {3, 4}, 
                    {3, 5}, 
                    {4, 5}
                }
            )
        }
    );
    path_trial_feature_graph = CreateTestGraph(
        "trial_f", 
        {
            VerticesSchema(
                "Person", 1024, 
                {
                    PropertySchema("hash_phone_no", "int32", false, true), 
                    PropertySchema("first_name", "string", false, false),
                    PropertySchema("last_name", "string", false, false)
                }, 
                {
                    {1, {{"hash_phone_no", int32_t{10}}, {"first_name", std::string{"Emily"}}, {"last_name", std::string{"Johnson"}}}}, 
                    {2, {{"hash_phone_no", int32_t{20}}, {"first_name", std::string{"James"}}, {"last_name", std::string{"Wilson"}}}}, 
                    {3, {{"hash_phone_no", int32_t{30}}, {"first_name", std::string{"Olivia"}}, {"last_name", std::string{"Brown"}}}}, 
                    {4, {{"hash_phone_no", int32_t{40}}, {"first_name", std::string{"Benjamin"}}, {"last_name", std::string{"Taylor"}}}}, 
                    {5, {{"hash_phone_no", int32_t{50}}, {"first_name", std::string{"Sophia"}}, {"last_name", std::string{"Martinez"}}}}
                }
            )
        }, 
        {
            EdgesSchema(
                "Person", "knows", "Person", 0, false, 
                {PropertySchema("friend_score", "int32", false, false), PropertySchema("created_at", "string", false, false), PropertySchema("tmp_", "float", false, false)}, 
                {
                    {1, 2, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                    {1, 3, {{"friend_score", int32_t{2}}, {"created_at", std::string{"2022-01-01"}}, {"tmp_", float{0.1}}}}, 
                    {2, 3, {{"friend_score", int32_t{3}}, {"created_at", std::string{"2021-11-01"}}, {"tmp_", float{0.1}}}}, 
                    {2, 4, {{"friend_score", int32_t{4}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                    {3, 4, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                    {3, 5, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                    {4, 5, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}
                }
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
