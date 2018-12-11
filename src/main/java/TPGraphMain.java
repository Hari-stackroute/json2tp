import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TPGraphMain {

    public static String getParent(JsonNode rootNode) {
        return rootNode.fieldNames().next();
    }

    // Expectation
    // Only one Grouping vertex = "teachers"  (plural of your parent vertex)
    // Multiple Parent vertex = teacher
    // Multiple child vertex = address
    // For every parent vertex and child vertex, there is a single Edge between
    //    teacher -> address
    public static void main(String[] args) {
        TPUtils.DBTYPE target = TPUtils.DBTYPE.NEO4J;
        int concurrencyLevel = 40;
        int currentLevel = 0;
        List<Thread> threadList = new ArrayList<>();

        //String jsonString = "{\"Teacher\": {  \"signatures\": {    \"@type\": \"sc:GraphSignature2012\",    \"signatureFor\": \"http://localhost:8080/serialNum\",    \"creator\": \"https://example.com/i/pat/keys/5\",    \"created\": \"2017-09-23T20:21:34Z\",    \"nonce\": \"2bbgh3dgjg2302d-d2b3gi423d42\",    \"signatureValue\": \"eyiOiJKJ0eXA...OEjgFWFXk\"      },  \"serialNum\": _SL_NUM_,  \"teacherCode\": \"_TC_\",  \"nationalIdentifier\": \"1234567890123456\",  \"teacherName\": \"_NAME_\",  \"gender\": \"GenderTypeCode-MALE\",  \"birthDate\": \"1990-12-06\",  \"socialCategory\": \"SocialCategoryTypeCode-GENERAL\",  \"highestAcademicQualification\": \"AcademicQualificationTypeCode-PHD\",  \"highestTeacherQualification\": \"TeacherQualificationTypeCode-MED\",  \"yearOfJoiningService\": \"2014\",  \"teachingRole\": {    \"@type\": \"TeachingRole\",    \"teacherType\": \"TeacherTypeCode-HEAD\",    \"appointmentType\": \"TeacherAppointmentTypeCode-REGULAR\",    \"classesTaught\": \"ClassTypeCode-SECONDARYANDHIGHERSECONDARY\",    \"appointedForSubjects\": \"SubjectCode-ENGLISH\",    \"mainSubjectsTaught\":       \"SubjectCode-SOCIALSTUDIES\",     \"appointmentYear\": \"2015\"      },  \"inServiceTeacherTrainingFromBRC\": {    \"@type\": \"InServiceTeacherTrainingFromBlockResourceCentre\",    \"daysOfInServiceTeacherTraining\": \"10\"      },  \"inServiceTeacherTrainingFromCRC\": {    \"@type\": \"InServiceTeacherTrainingFromClusterResourceCentre\",    \"daysOfInServiceTeacherTraining\": \"2\"      },  \"inServiceTeacherTrainingFromDIET\": {    \"@type\": \"InServiceTeacherTrainingFromDIET\",    \"daysOfInServiceTeacherTraining\": \"5.5\"      },  \"inServiceTeacherTrainingFromOthers\": {    \"@type\": \"InServiceTeacherTrainingFromOthers\",    \"daysOfInServiceTeacherTraining\": \"3.5\"      },  \"nonTeachingAssignmentsForAcademicCalendar\": {    \"@type\": \"NonTeachingAssignmentsForAcademicCalendar\",    \"daysOfNonTeachingAssignments\": \"6\"      },  \"basicProficiencyLevel\":         {      \"@type\": \"BasicProficiencyLevel\",      \"proficiencySubject\": \"SubjectCode-MATH\",      \"proficiencyAcademicQualification\": \"AcademicQualificationTypeCode-PHD\"        },  \"disabilityType\": \"DisabilityCode-NA\",  \"trainedForChildrenSpecialNeeds\": \"YesNoCode-YES\",  \"trainedinUseOfComputer\": \"YesNoCode-YES\"    }}";
        String jsonString = "{\"A\":{ \"B1\": {\"C\":\"D\"}, \"B2\": \"C2\"}}";

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(jsonString);
            String rootName = getParent(rootNode);
            System.out.println("Parent Name = " + rootName);


            // Check the rootVertex
            Vertex rootVertex;
            // Just to check if we could have a valid connection
            try (Graph graph = TPUtils.getGraph(target)) {
                try (Transaction tx = graph.tx()) {
                    rootVertex = TPUtils.createParentVertex(graph, rootName + "_GROUP");
                    tx.commit();
                }
            }

            for (int i = 1; i <= 1; i++) {
                jsonString = jsonString.replace("_NAME_", "John" + i);
                jsonString = jsonString.replace("_SL_NUM_", "" + i);
                jsonString = jsonString.replace("_TC_", "_TC_" + i);

                CreateRecord cr = new CreateRecord(rootNode, rootName, target, rootVertex, i);

                if (currentLevel != concurrencyLevel) {
                    Thread thread = new Thread(cr);
                    thread.start();
                    threadList.add(thread);
                    currentLevel++;
                } else {
                    threadList.forEach(thread -> {
                        try {
                            thread.join(5000);

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                    threadList.clear();
                    currentLevel = 0;
                    System.out.println(i + " Cleared all threads");
                }
            }
        } catch (IOException ioe) {
            System.out.println("Can't read json " + ioe);
        } catch (Exception e) {
            System.out.println("Can't close autocloseable " + e);
        }
    }
}
