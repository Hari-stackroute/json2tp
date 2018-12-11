import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.umlg.sqlg.structure.SqlgGraph;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class TPGraphMain {
    private static Graph graph;
    private static Driver driver;

    public static Graph createNeo4jGraph() {
        boolean withAuth = true;
        String databaseHost = "localhost";
        String databasePort = "7687";
        /*Boolean profilerEnabled = Boolean
                .parseBoolean(environment.getProperty("database.neo4j.profiler_enabled"));*/
        AuthToken authToken = AuthTokens.basic("neo4j", "stackroute1!");

        if (withAuth) {
            driver = GraphDatabase.driver(String.format("bolt://%s:%s", databaseHost, databasePort),
                    authToken);
        } else {
            driver = GraphDatabase.driver(String.format("bolt://%s:%s", databaseHost, databasePort),
                    AuthTokens.none());
        }
        //Neo4JElementIdProvider<?> idProvider = new Neo4JNativeElementIdProvider();
        Neo4JElementIdProvider<?> idProvider = new RecordIdProvider();
        Neo4JGraph neo4JGraph = new Neo4JGraph(driver, idProvider, idProvider);
        //neo4JGraph.setProfilerEnabled(profilerEnabled);
        graph = neo4JGraph;

        return graph;
    }

    public static Graph createPostgresGraph() {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/json2tp";
        String jdbcUsername = "postgres";
        String jdbcPassword = "postgres";
        Configuration config = new BaseConfiguration();
        config.setProperty("jdbc.url", jdbcUrl);
        config.setProperty("jdbc.username", jdbcUsername);
        config.setProperty("jdbc.password", jdbcPassword);
        graph = SqlgGraph.open(config);
        return graph;
    }

    public static String createLabel() {
        return UUID.randomUUID().toString();
    }

    public static Object createVertex(Graph graph, String label, Vertex parentVertex, JsonNode jsonObject) {
        Vertex vertex = graph.addVertex(label);
        vertex.property("osid", vertex.id());
        jsonObject.fields().forEachRemaining(entry -> {
            JsonNode entryValue = entry.getValue();
            if (entryValue.isValueNode()) {
                vertex.property(entry.getKey(), entryValue.asText());
            } else if (entryValue.isObject()) {
                createVertex(graph, entry.getKey(), vertex, entryValue);
            }
        });
        Edge e = addEdge(graph, label, parentVertex, vertex);
        return e.id();
    }

    public static Edge addEdge(Graph graph, String label, Vertex v1, Vertex v2) {
        return v1.addEdge(label, v2);
    }

    public static Vertex createParentVertex() {
        String personsStr = "Persons";
        String personsId = "ParentEntity_Persons";
        GraphTraversalSource gtRootTraversal = graph.traversal();
        GraphTraversal<Vertex, Vertex> rootVertex = gtRootTraversal.V().hasLabel(personsStr);
        Vertex parentVertex = null;
        if (!rootVertex.hasNext()) {
            parentVertex = graph.addVertex(personsStr);
            parentVertex.property("osid", parentVertex.id());
            parentVertex.property("label", personsStr);
        } else {
            parentVertex = rootVertex.next();
        }

        return parentVertex;
    }

    public static List<String> verticesCreated = new ArrayList<String>();

    public static void processNode(String parentName, Vertex parentVertex, JsonNode node) {
        Iterator<Map.Entry<String, JsonNode>> entryIterator = node.fields();
        while (entryIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = entryIterator.next();
            if (entry.getValue().isValueNode()) {
                // Create properties
                System.out.println("Create properties within vertex " + parentName);
                System.out.println(parentName + ":" + entry.getKey() + " --> " + entry.getValue());
                parentVertex.property(entry.getKey(), entry.getValue());
            } else if (entry.getValue().isObject()) {
                Object edgeid = createVertex(graph, entry.getKey(), parentVertex, entry.getValue());
                parentVertex.property(entry.getKey() + "_osid", edgeid);
            } else if (entry.getValue().isArray()) {
                // TODO
            }
        }
    }

    public static enum DBTYPE {NEO4J, POSTGRES};


    // Expectation
    // Only one Grouping vertex = "teachers"  (plural of your parent vertex)
    // Multiple Parent vertex = teacher
    // Multiple child vertex = address
    // For every parent vertex and child vertex, there is a single Edge between
    //    teacher -> address
    public static void main(String[] args) throws IOException {
        DBTYPE target = DBTYPE.NEO4J;

        if (target == DBTYPE.NEO4J) {
            createNeo4jGraph();
        } else if (target == DBTYPE.POSTGRES){
            createPostgresGraph();
        }

        for (int i = 0; i < 10; i++) {
            String jsonString = "{\"teacher\": {\"firstName\":\"person_fn\",\"lastName\":\"person_ln\", \"address\": {\"door\": 10}}}";
            jsonString = jsonString.replace("person_fn", "John" + i);
            jsonString = jsonString.replace("person_ln", "Ram" + i);

            Instant startTime = Instant.now();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonString);
            Transaction tx = graph.tx();
            processNode(null, createParentVertex(), rootNode);
            tx.commit();

            Instant endTime = Instant.now();
            System.out.println(i + "," +
                    Duration.between(startTime, endTime).toNanos() + "," +
                    Duration.between(startTime, endTime).toMillis() + "," +
                    Duration.between(startTime, endTime).toMinutes());
        }
    }
}
