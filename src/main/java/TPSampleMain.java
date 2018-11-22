import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import java.io.IOException;
import java.util.*;

public class TPSampleMain {
    private static Graph graph;
    private static Driver driver;
    public static Graph createGraph() {
        String databaseHost = "localhost";
        String databasePort = "7687";
        /*Boolean profilerEnabled = Boolean
                .parseBoolean(environment.getProperty("database.neo4j.profiler_enabled"));*/
        driver = GraphDatabase.driver(String.format("bolt://%s:%s", databaseHost, databasePort),
                AuthTokens.none());
        Neo4JElementIdProvider<?> idProvider = new Neo4JNativeElementIdProvider();
        Neo4JGraph neo4JGraph = new Neo4JGraph(driver, idProvider, idProvider);
        //neo4JGraph.setProfilerEnabled(profilerEnabled);
        graph = neo4JGraph;
        return graph;
    }

    /*public static Graph addVertex(Graph g, Vertex v) {

    }*/

    public static String createLabel(){
        return UUID.randomUUID().toString();
    }

    public static Object createVertex(Graph g, String label, Vertex parentVertex,JsonNode jsonObject) {

        Vertex vertex = g.addVertex(createLabel());
        jsonObject.fields().forEachRemaining(entry -> {
            JsonNode entryValue =  entry.getValue();
            if(entryValue.isValueNode()){
                vertex.property(entry.getKey(), entryValue.asText());
            } else if(entryValue.isObject()){
                createVertex(g,entry.getKey(),vertex,entryValue);
            }
        });
        Edge e = addEdge(g,label,vertex,parentVertex);
        return e.id();
    }

    public static Edge addEdge(Graph g,String label, Vertex v1, Vertex v2) {
        return v1.addEdge(label,v2);
    }

    public static Edge addProperty(Graph g,String label, Vertex v1, Vertex v2) {
        return v1.addEdge(label,v2);
    }

    public static Vertex createParentVertex(Graph g){
            return g.addVertex("Person");
            //return parentVertex;
    }

    public static List<String> verticesCreated = new ArrayList<String>();

    public static Vertex parentVertex = null;

    public static void processNode(String parentName, JsonNode node) {
        //Vertex parentVertex = null;
        List<Object> idLst = new ArrayList<Object>();
       /* if (parentName!= null && !verticesCreated.contains(parentName)) {
            System.out.println("Create Vertex " + parentName);
            verticesCreated.add(parentName);*/
            parentVertex =  createParentVertex(g);
       /* }*/

        Iterator<Map.Entry<String, JsonNode>> entryIterator = node.fields();
        while (entryIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = entryIterator.next();
           if (entry.getValue().isValueNode()) {
                // Create properties
                System.out.println("Create properties within vertex " + parentName);
                System.out.println( parentName + ":" + entry.getKey() + " --> " + entry.getValue());
                parentVertex.property(entry.getKey(),entry.getValue());
            } else if (entry.getValue().isObject()) {
               // processNode(entry.getKey(), entry.getValue());
               if(parentVertex == null){
                   parentVertex =  createParentVertex(g);
               }
               Object edgeid = createVertex(g,entry.getKey(),parentVertex,entry.getValue());
               parentVertex.property("id",idLst.add(edgeid));
            } else if (entry.getValue().isArray()) {
                // TODO
            }
        }
    }

    private static Graph g;

    public static void main(String[] args) throws IOException {
        String jsonString = "{\"teacher\": {\"firstName\":\"hari\",\"lastName\":\"Palemkota\", \"address\": {\"door\": 10}}}";
        JsonNode parentNode ;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        Iterator it = actualObj.iterator();
        g = createGraph();
        Vertex parentVertex = null;
        List<Object> idLst = null;
        List<VertexProperty> vertexPropertyLst = new ArrayList<VertexProperty>();
        g.addVertex("hari");
       // processNode(null, actualObj);

        /*while(it.hasNext()){
            JsonNode entryValue = (JsonNode) it.next();
            if(entryValue.isValueNode()){
                if(parentVertex == null){
                    parentVertex = createParentVertex(g);
                }
                System.out.println(it.toString() + "==>" + entryValue.asText());
                parentVertex.property(it.toString() ,entryValue.asText());
            } else if(entryValue.isObject()){
                System.out.println("Vertex " +  it.toString() + "==>" + entryValue.asText());
                Object edgeid = createVertex(g,it.toString(),parentVertex,entryValue);
                parentVertex.property("id",idLst.add(edgeid));
            }
        }
        parentVertex = createParentVertex(g);*/

//
//        actualObj.fields().forEachRemaining(entry -> {
//            JsonNode entryValue =  entry.getValue();
//            if(entryValue.isValueNode()){
//
//                   // parentVertex = createParentVertex(g);
//                addProperty(parentVertex,entry.getKey(),entryValue.asText());
//                //parentVertex.property(entry.getKey(),entryValue.asText());
//            } else if(entryValue.isObject()){
//                Object edgeid = createVertex(g,entry.getKey(),parentVertex,entryValue);
//                parentVertex.property("id",idLst.add(edgeid));
//            }
//        });
       /* while(it.hasNext()){
            List<String> props = new ArrayList<String>();
            JsonNode jsonNode1 = (JsonNode) it.next();
            if (jsonNode1.isValueNode()) {
                // add to a property

                jsonNode1.toString(),

            } else if (jsonNode1.isArray()) {
                // for each element in the array, do create vertex or add Edge

            } else if (jsonNode1.isObject()) {
                Vertex v = createVertex()
            }
        }*/
        System.out.println(actualObj);
    }
}
