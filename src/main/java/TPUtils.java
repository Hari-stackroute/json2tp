import com.fasterxml.jackson.databind.JsonNode;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.neo4j.driver.v1.*;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class TPUtils {
    private static Driver driver;
    private static void initDriver(boolean withAuth, String databaseHost, String databasePort) {
        Config.ConfigBuilder configBuilder = Config.build().withConnectionTimeout(1, TimeUnit.MINUTES)
                .withMaxIdleSessions(10)
                .withConnectionLivenessCheckTimeout(30, TimeUnit.SECONDS);
        Config config = configBuilder.toConfig();

        if (withAuth) {
            AuthToken authToken = AuthTokens.basic("neo4j", "stackroute1!");
            driver = GraphDatabase.driver(String.format("bolt://%s:%s", databaseHost, databasePort),
                    authToken, config);
        } else {
            driver = GraphDatabase.driver(String.format("bolt://%s:%s", databaseHost, databasePort),
                    AuthTokens.none(), config);
        }
    }


    public static Graph createNeo4jGraph() {
        boolean withAuth = false;
        String databaseHost = "localhost";
        String databasePort = "7687";
        /*Boolean profilerEnabled = Boolean
                .parseBoolean(environment.getProperty("database.neo4j.profiler_enabled"));*/
        if (driver == null) { initDriver(withAuth, databaseHost, databasePort); }

        Neo4JElementIdProvider<?> idProvider = new Neo4JNativeElementIdProvider();
        //Neo4JElementIdProvider<?> idProvider = new RecordIdProvider();
        Neo4JGraph neo4JGraph = new Neo4JGraph(driver, idProvider, idProvider);
        //neo4JGraph.setProfilerEnabled(profilerEnabled);

        return neo4JGraph;
    }

    public static Graph createPostgresGraph() {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/json2tp";
        String jdbcUsername = "postgres";
        String jdbcPassword = "postgres";
        Configuration config = new BaseConfiguration();
        config.setProperty("jdbc.url", jdbcUrl);
        config.setProperty("jdbc.username", jdbcUsername);
        config.setProperty("jdbc.password", jdbcPassword);
        SqlgGraph sqlgGraph = SqlgGraph.open(config);

        return sqlgGraph;
    }

    public static Graph getGraph(DBTYPE target) {
        switch (target) {
            case NEO4J:
                return createNeo4jGraph();
            case POSTGRES:
                return createPostgresGraph();
            default:
                return null;
        }
    }

    public static String createLabel() {
        return UUID.randomUUID().toString();
    }

    public static Vertex createVertex(Graph graph, String label) {
        Vertex vertex = graph.addVertex(label);
        vertex.property("osid", vertex.id());
        return vertex;
    }

    /**
     *
     * @param label
     * @param srcV - Source vertex
     * @param tgtV - Target vertex
     * @param inOrOut
     */
    public static void addEdge(String label, Vertex srcV, Vertex tgtV, Direction inOrOut) {
        if (inOrOut == Direction.OUT) {
            Edge edge = srcV.addEdge(label, tgtV);
            edge.property("eid", edge.id());
            srcV.property(tgtV.label() + "id", edge.id());
        } else if (inOrOut == Direction.IN) {
            tgtV.addEdge(label + "_edge", srcV);
            tgtV.addEdge("temp", srcV);
            //edge.property("eid", edge.id());
            //tgtV.property(srcV.label() + "id", edge.id());
        }
    }

    public static Vertex createParentVertex(Graph graph, String parentLblName, String idToSet) {
        GraphTraversalSource gtRootTraversal = graph.traversal();
        GraphTraversal<Vertex, Vertex> rootVertex = gtRootTraversal.V().hasLabel(parentLblName);
        Vertex parentVertex = null;
        if (!rootVertex.hasNext()) {
            parentVertex = graph.addVertex(parentLblName);
            parentVertex.property("pid", idToSet);
            parentVertex.property("label", parentLblName);
        } else {
            parentVertex = rootVertex.next();
        }

        return parentVertex;
    }

    public static void processNode(Graph graph, String entityType, Vertex groupingVertex, JsonNode node) {
        Vertex record = createVertex(graph, entityType);
        addEdge(entityType, groupingVertex, record, Direction.IN);

//        jsonObject.fields().forEachRemaining(entry -> {
//            JsonNode entryValue = entry.getValue();
//            if (entryValue.isValueNode()) {
//                vertex.property(entry.getKey(), entryValue.asText());
//            } else if (entryValue.isObject()) {
//                createVertex(graph, entry.getKey(), vertex, entryValue);
//            } else if (entry.getValue().isArray()) {
//                // TODO
//            }
//        });
    }

    public static enum DBTYPE {NEO4J, POSTGRES}

    private DBTYPE target;

    void setTarget(DBTYPE dbtype) {
        target = dbtype;
    }

}
