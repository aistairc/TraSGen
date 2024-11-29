package DataGen.utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.locationtech.jts.geom.Coordinate;

public final class JsonStringConversions {
    
    public static String edgeTrafficSink(String edgeName1, Integer trafficCount1, String edgeName2, Integer trafficCount2,
                                         Long currentTimeMillis, Long seqID, Integer trajStatus, Integer objID, Long batchID,
                                         Coordinate position , Double speed, Double azimuth) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        root.put("edgeName1", edgeName1);
        root.put("trafficCount1", trafficCount1);
        root.put("edgeName2", edgeName2);
        root.put("trafficCount2", trafficCount2);
        root.put("currentTimeMillis", currentTimeMillis);
        root.put("seqID", seqID);
        root.put("trajStatus", trajStatus);
        root.put("objID", objID);
        root.put("batchID", batchID);
        root.put("x", position.x);
        root.put("y", position.y );
        root.put("speed", speed);
        root.put("azimuth", azimuth);

        return root.toString();
    }


}
