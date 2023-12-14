package DataGen.timeSeriesGenerators;

import DataGen.timeSeriesGenerators.network.NetworkDistribution;
import DataGen.utils.HelperClass;
import DataGen.utils.NetworkPath;
import DataGen.utils.Serialization;
import DataGen.utils.SpatialFunctions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


public class NetworkPointStreamGeneratorSync1tuple implements StreamGenerator, Serializable  {

    private Map<Integer, GraphPath<String, DefaultWeightedEdge>> shortestIDPathMap = new HashMap<>();
    private NetworkPath networkPath = null;
    private final NetworkDistribution networkDistribution;
    private String outputFormat = "GeoJSON";


    private transient StreamExecutionEnvironment env = null;
    private Properties kafkaProperties = null;


    private  CoordinateReferenceSystem crs;
    private final double displacementMetersPerSecond;

    private double syncPercentage;

    HashSet<Integer> objIDList = new LinkedHashSet<Integer>();

    HashSet<Integer> trafficTupleSet = new LinkedHashSet<Integer>();

    int c1 = 0;
    int c2 = 0;

    Random random = new Random();

    private String initialTimeStamp = null;

    private String dateFormat = null;

    private int timeStepinMilliSec = 0;
    boolean randomizeTimeInBatch;



    public NetworkPointStreamGeneratorSync1tuple(NetworkDistribution networkDistribution, Properties kafkaProperties, StreamExecutionEnvironment env, String outputFormat,
                                                 String mapFile, String mapFileFormat, String shortestPathAlgorithmStr, Double nodeMappingTolerance,
                                                 int minObjID, int maxObjID, String trajStartEndSelectionApproach, List<List<Double>> trajStartEndCoordinatePairs,
                                                 List<List<Double>> trajStartPolygons, List<List<Double>> trajEndPolygons, double displacementMetersPerSecond, CoordinateReferenceSystem crs,
                                                 int parallelism,  double syncPercentage, String dateFormat, String initialTimeStamp, int timeStepinMilliSec, boolean randomizeTimeInBatch){


        this.networkDistribution = networkDistribution;
        this.outputFormat = outputFormat;

        networkPath = new NetworkPath();
        // Define defaultWeightedEdgeGraph based on parameters
        Graph<String, DefaultWeightedEdge> defaultWeightedEdgeGraph = networkPath.defineNetwork(mapFile, mapFileFormat, nodeMappingTolerance);
        // Defining Shortest Path Algorithm On Graph
        networkPath.defineShortestPathOnGraph(defaultWeightedEdgeGraph, shortestPathAlgorithmStr);
        // Set initial shortest network paths
        shortestIDPathMap = networkPath.setInitialShortestTrajs(minObjID, maxObjID, trajStartEndSelectionApproach, trajStartEndCoordinatePairs, trajStartPolygons, trajEndPolygons);

        this.env = env;
        this.kafkaProperties = kafkaProperties;
        this.crs = crs;
        this.displacementMetersPerSecond = displacementMetersPerSecond;
        this.syncPercentage = syncPercentage;
        this.initialTimeStamp = initialTimeStamp;
        this.timeStepinMilliSec = timeStepinMilliSec;
        this.dateFormat = dateFormat;
        this.randomizeTimeInBatch = randomizeTimeInBatch;

        int totalObjIDs =  maxObjID - minObjID + 1;
        for(int i = minObjID ; i < totalObjIDs + 1 ; i++)
            this.objIDList.add(i);
    }

    @Override
    public DataStream<String> generate(DataStream<Tuple2<Integer,Long>> objIDStream) {

        //read edge information from Kafka topic Feedback
        DataStream<String> edgeTrafficCountString = this.env.addSource(new FlinkKafkaConsumer<>("Feedback", new SimpleStringSchema(), this.kafkaProperties));
        //Deserialize String -> Tuple2
        //EdgeName, TrafficCount, EdgeName, TrafficCount, currentTimeMillis(), seqID, TrajStatus, objID

        DataStream<Tuple9<String, Integer, String, Integer, Long, Long, Integer,Integer, Long>> edgeTrafficCount = edgeTrafficCountString.map(new MapFunction<String, Tuple9<String, Integer, String, Integer, Long, Long, Integer,Integer, Long>>() {
            @Override
            public Tuple9<String, Integer, String, Integer, Long, Long, Integer,Integer, Long> map(String str) throws Exception {
                String[] temp = str.split(",");
                return Tuple9.of(temp[0], Integer.parseInt(temp[1]), temp[2], Integer.parseInt(temp[3]), Long.parseLong(temp[4]), Long.parseLong(temp[5]), Integer.parseInt(temp[6]), Integer.parseInt(temp[7]), Long.parseLong(temp[8]));
            }
        });

        KeyedStream<Tuple2<Integer,Long>, Integer> keyedobjIDStream = objIDStream.keyBy(new HelperClass.objIDKeySelectorWithBatchID());

        MapStateDescriptor<String,Tuple3<Integer, Long, Integer>> broadcastStateDescriptor =
                new MapStateDescriptor<>("edgeTrafficMap", BasicTypeInfo.STRING_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Long.class,Integer.class));
        MapStateDescriptor<String, Tuple4<Long, Long, Long, Long>> syncStateDescriptor = new MapStateDescriptor<>("syncState", BasicTypeInfo.STRING_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Long.class, Long.class, Long.class, Long.class));
        MapStateDescriptor<String,HashSet<Integer>> objIDStateDesc = new MapStateDescriptor<>("objIDState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));
        MapStateDescriptor<String,HashSet<Integer>> removeIDStateDesc = new MapStateDescriptor<>( "removeIDState",  BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));
        MapStateDescriptor<String,HashSet<Integer>> expectedobjIDStateDesc = new MapStateDescriptor<>( "expectedobjIDState",  BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));
//

        // String, Integer, String, Integer, Long, Long, Integer, Long
        BroadcastStream<Tuple9<String, Integer, String, Integer, Long, Long, Integer, Integer, Long>> broadcastTrafficMap = edgeTrafficCount.broadcast(broadcastStateDescriptor,syncStateDescriptor, objIDStateDesc, removeIDStateDesc, expectedobjIDStateDesc); //Broadcast edgeTrafficCount
        //OutputTag required to generate side output datastream sent to be written to Feedback
        OutputTag<Tuple9<String, Integer, String, Integer, Long, Long,Integer, Integer, Long>> outputTag = new OutputTag<Tuple9<String, Integer, String, Integer, Long, Long, Integer, Integer, Long>>("feedback-sideoutput"){};  // String, Integer, String, Integer, Long, Long, Integer, Long
        //      0              1                  2                 3           4           5
        // "syncState",expectedBatchCounts, totalBatchCount, currBatchCount, currbatchID, removeIDList;
        OutputTag<Tuple6<String, Long, Long, Long, Long, HashSet<Integer>>> controlTupleTag = new OutputTag<Tuple6<String, Long, Long, Long, Long, HashSet<Integer>>>("controlTuples"){};
        SingleOutputStreamOperator<String> networkPoints = keyedobjIDStream.connect(broadcastTrafficMap).process(new NetworkBroadcastProcessFunctionSync1tuple<Coordinate>(Coordinate.class, networkDistribution, this.shortestIDPathMap, this.crs, this.displacementMetersPerSecond, this.random) {

            @Override
            public void processElement(Tuple2<Integer, Long>  objID,  ReadOnlyContext ctx, Collector<String> collector) throws Exception {

                LocalDateTime localDateTime = LocalDateTime.now();
                GraphPath<String, DefaultWeightedEdge> shortestPath = this.shortestIDPathMap.get(objID.f0);
                List<DefaultWeightedEdge> shortestPathEdgeList = shortestPath.getEdgeList();

                Integer currentEdgeIndex;
                DefaultWeightedEdge currentEdge;
                DefaultWeightedEdge oldEdge;
                Coordinate outputPointCoordinates = null;
                Coordinate lastPointCoordinates;
                Double lastAzimuth;
                Integer currentRoadTraffic = 0;
                Double currentDisplacementPerUnitTime =  0.0;

                Long batchID = objID.f1;
                boolean condition = true;

                ReadOnlyBroadcastState<String,Tuple8<Integer, Long, Integer, Long, Long, Integer, Long, Long>> bcState = ctx.getBroadcastState(this.edgeTrafficMapDesc);
                ReadOnlyBroadcastState<String,HashSet<Integer>> expectedobjIDState = ctx.getBroadcastState(this.expectedobjIDState);
                ReadOnlyBroadcastState<String,HashSet<Integer>> objIDState = ctx.getBroadcastState(this.objIDState);

                if (expectedobjIDState.get("expectedobjIDState") != null ) {
                    trafficTupleSet = expectedobjIDState.get("expectedobjIDState");
//                    System.out.println("trafficTupleSet  exists! ");
                    condition = trafficTupleSet.contains(objID.f0);
                }

                // Retrieving edge using edge index
                if (currentEdgeIndexVState != null && currentEdgeIndexVState.value() != null) {

                    currentEdgeIndex = currentEdgeIndexVState.value();

                    if (currentEdgeIndex >= shortestPathEdgeList.size()) {          // if traj has reached the end.
                        if (condition)
                        {ctx.output(outputTag, Tuple9.of("update_time", 0, "update_time", 0,  System.currentTimeMillis(), seqID.value(), -1, objID.f0, objID.f1 ));}  // dummy
                        return;
                    } else {
                        currentEdge = shortestPathEdgeList.get(currentEdgeIndex);
                    }
                    //currentEdge = shortestPathEdgeList.get(currentEdgeIndex);

                } else { // Setting the initial value of currentEdgeIndex

                    currentEdgeIndex = 0;
                    currentEdge = shortestPathEdgeList.get(0);
                    currentEdgeIndexVState.update(currentEdgeIndex);

                    if (condition)
                    { ctx.output(outputTag, Tuple9.of(currentEdge.toString(), 1, currentEdge.toString(), 0, System.currentTimeMillis(), seqID.value(), 1, objID.f0, objID.f1));}

                }
                // If one or more trajectory tuples already generated
                if (lastGeometryVState != null && lastGeometryVState.value() != null && lastAzimuthVState != null && lastAzimuthVState.value() != null) {
//)
                    lastPointCoordinates = this.lastGeometryVState.value();
                    lastAzimuth = this.lastAzimuthVState.value();

                    String edgeTarget = shortestPath.getGraph().getEdgeTarget(currentEdge);
                    Coordinate edgeTargetCoordinates = networkPath.getNodeCoordinate(edgeTarget);
                    String edgeSource =  shortestPath.getGraph().getEdgeSource(currentEdge);
                    Coordinate edgeSourceCoordinates = networkPath.getNodeCoordinate(edgeSource);

                    // If the remaining distance on edge is less than NETWORK_DISPLACEMENT_METERS, return the edge target coordinate
                    double remainingDistOnEdge = SpatialFunctions.getDistanceInMeters(lastPointCoordinates, edgeTargetCoordinates, this.crs, this.gc);

                    //System.out.println("remainingDistOnEdge " + remainingDistOnEdge);
                    if (bcState.get(currentEdge.toString()) != null) {
                        currentRoadTraffic = bcState.get(currentEdge.toString()).f0;
                    }

                    currentDisplacementPerUnitTime = HelperClass.getDisplacementMetersPerSecond(ROAD_CAPACITY, edgeSourceCoordinates,edgeTargetCoordinates, currentRoadTraffic, this.displacementMetersPerSecond, this.crs, this.gc);
//                    if (currentDisplacementPerUnitTime != 16.0) {System.out.println(currentDisplacementPerUnitTime);}
                    if (remainingDistOnEdge <= currentDisplacementPerUnitTime) {
                        outputPointCoordinates = edgeTargetCoordinates;
                        currentEdgeIndex++;
                        currentEdgeIndexVState.update(currentEdgeIndex);

                        // Return the currentEdge as the past edge if number of edges have exhausted
                        if (currentEdgeIndex >= shortestPathEdgeList.size()) {
//
                            if (condition)
                            {ctx.output(outputTag, Tuple9.of(currentEdge.toString(), 0, currentEdge.toString(), -1, System.currentTimeMillis(), seqID.value(), -1, objID.f0, objID.f1));}
                            return;
                        } else {
                            oldEdge = currentEdge;
                            currentEdge = shortestPathEdgeList.get(currentEdgeIndex);
                            if (condition)
                            {ctx.output(outputTag, Tuple9.of(oldEdge.toString(), -1, currentEdge.toString(), 1, System.currentTimeMillis(), seqID.value(), 0, objID.f0, objID.f1));}

                        }

                        // new edgeTarget as currentEdgeIndex has changed
                        edgeTarget = shortestPath.getGraph().getEdgeTarget(currentEdge).toString();
                        edgeTargetCoordinates = networkPath.getNodeCoordinate(edgeTarget);

                        Double edgeAzimuth = SpatialFunctions.getAzimuthInDecimalDegrees(outputPointCoordinates, edgeTargetCoordinates, this.crs,this.gc);

                        lastAzimuthVState.update(edgeAzimuth);
                        lastGeometryVState.update(outputPointCoordinates);

                    } else {
                        outputPointCoordinates = networkDistribution.next(lastPointCoordinates, lastAzimuth, currentDisplacementPerUnitTime, this.crs, this.gc);
                        lastGeometryVState.update(outputPointCoordinates);
                        if (condition)
                        {ctx.output(outputTag, Tuple9.of(currentEdge.toString(), 0, currentEdge.toString(), 0, System.currentTimeMillis(), seqID.value(), 0,  objID.f0, objID.f1));}  // dummy
                    }

                    seqID.update(seqID.value() + 1);


                } else { // If this is the first trajectory tuple

                    String edgeSource = shortestPath.getGraph().getEdgeSource(currentEdge).toString();
                    Coordinate edgeSourceCoordinates = networkPath.getNodeCoordinate(edgeSource);

                    String edgeTarget = shortestPath.getGraph().getEdgeTarget(currentEdge).toString();
                    Coordinate edgeTargetCoordinates = networkPath.getNodeCoordinate(edgeTarget);

                    Double edgeAzimuth = SpatialFunctions.getAzimuthInDecimalDegrees(edgeSourceCoordinates, edgeTargetCoordinates, this.crs, this.gc);

                    outputPointCoordinates = edgeSourceCoordinates;

                    seqID.update(seqID.value() + 1);

                    lastGeometryVState.update(edgeSourceCoordinates);
                    lastAzimuthVState.update(edgeAzimuth);
                }

                if (outputFormat.equals("GeoJSON")) {
                        collector.collect(Serialization.generatePointJson(
                                outputPointCoordinates.x, outputPointCoordinates.y, objID.f0, seqID.value(),
                                currentEdge.toString().replaceAll("[\\p{Ps}\\p{Pe}]", ""),
                                currentRoadTraffic, currentDisplacementPerUnitTime,
                                HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)).toString());

                } else {
                    collector.collect(Serialization.generateGeometryWKT(HelperClass.generatePoint(outputPointCoordinates), objID.f0, seqID.value(),
                            HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)));
                }
            }
                // return for testing purpose
                // return Serialization.generatePointJson( 10.0, 10.0, objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString();


            @Override
            //EdgeName, TrafficCount, EdgeName, TrafficCount, currentTimeMillis(), seqID, TrajStatus, objID, batchID
            public void processBroadcastElement(Tuple9<String, Integer, String, Integer, Long, Long, Integer,Integer, Long> edgeTraffic, Context ctx, Collector<String> collector) throws Exception {
                BroadcastState<String,Tuple8<Integer, Long, Integer,Long, Long, Integer, Long, Long>>  bcState = ctx.getBroadcastState(this.edgeTrafficMapDesc);
                BroadcastState<String, Tuple4<Long, Long, Long, Long>> syncState = ctx.getBroadcastState(this.syncState);
                BroadcastState<String,HashSet<Integer>> removeIDState = ctx.getBroadcastState(this.removeIDState);
                BroadcastState<String,HashSet<Integer>> objIDState = ctx.getBroadcastState(this.objIDState);
                BroadcastState<String,HashSet<Integer>> expectedobjIDState = ctx.getBroadcastState(this.expectedobjIDState);
                //                                  0                   1               2               3
                // "syncState", Tuple5.of(expectedBatchCount, totalBatchCount, currBatchCount, currbatchID));

//                System.out.println(edgeTraffic.toString());

                String edge1 = edgeTraffic.f0;
                Integer traffic1 = edgeTraffic.f1;
                String edge2 = edgeTraffic.f2;
                Integer traffic2 = edgeTraffic.f3;
                Long sideoutputTimestamp = edgeTraffic.f4;
                Long seqID = edgeTraffic.f5;
                Integer trajStatus = edgeTraffic.f6;
                Integer objID = edgeTraffic.f7.intValue();
                Long batchID = edgeTraffic.f8;
                Long currentTime;
                Long deltaT;

                Integer totalEdgeTraffic1 = 0;
                Integer totalEdgeTraffic2 = 0;

                // for syncState
                Long currBatchCount = 0L;
                Long totalBatchCount = 0L;
                Long currBatchID = 1L;

                HashSet<Integer> removeIDs;
                HashSet<Integer> objIDs;
                Long expectedBatchCount = 0L;

                if (objIDState.contains("objIDState")) {
                    objIDs = objIDState.get("objIDState");
                } else {
                    objIDs =  objIDList;
                    expectedBatchCount =  (long) Math.floor(objIDs.size() * syncPercentage/100.0);
                    if ( (expectedBatchCount.equals(0L))) {expectedBatchCount = 1L;}
                }

                if (removeIDState.contains("removeIDState")) {
                    removeIDs = removeIDState.get("removeIDState");
                } else {
                    removeIDs =  new LinkedHashSet<Integer>();
                }

                if (syncState.contains("syncState")) {
                    currBatchID = syncState.get("syncState").f3;
                    currBatchCount = syncState.get("syncState").f2;
                    totalBatchCount = syncState.get("syncState").f1;
                    expectedBatchCount = syncState.get("syncState").f0;
                }
                // add to list of trajs to be removed
                if (trajStatus.intValue() == -1) {
                    removeIDs.add(objID);
                }

                totalBatchCount = totalBatchCount + 1L;
                // increment batch count
                if (batchID.equals(currBatchID)) {
                    currBatchCount = currBatchCount + 1L;
                }
//
//                System.out.println("currBatchCount: " + currBatchCount);
//                System.out.println("expectedBatchCount: " + expectedBatchCount);

                if (currBatchCount.equals(expectedBatchCount)) {
                    /// release control tuple
                    ctx.output(controlTupleTag, Tuple6.of("syncState" , expectedBatchCount, totalBatchCount, currBatchCount, currBatchID, removeIDs));
                    // remove IDs from obj list
                    for (Integer removeID : removeIDs) {
                        objIDs.remove(removeID);
                    }


                    //Handle when objIDs becomes empty but manager still sending objIDs (end of trajectories)
                    if (objIDs.size() == 0){
                        objIDs.add(objID);
//                        System.out.println("Empty obj IDs: " + objIDs.toString());
                    }

                    // calc new expected batch count
                    expectedBatchCount = (long) Math.floor((objIDs.size() * syncPercentage/100.0));
                    if ((expectedBatchCount.equals(0L))) {expectedBatchCount = 1L;}
                    removeIDs.clear();
                    currBatchCount = 0L; //reset
                    currBatchID = currBatchID + 1L; // increment expected batchID
                    objIDState.put("objIDState", objIDs);

////                // expected OBJ ID Traffic tuple list selection
                    List<Integer> list = new LinkedList<Integer>(objIDs);
                    int startIndexRange = list.size() - expectedBatchCount.intValue();
                    int startIndex = startIndexRange == 0 ? 0 : random.nextInt(startIndexRange);
                    HashSet<Integer> smallSet = new HashSet<Integer>(list.subList(startIndex, expectedBatchCount.intValue() + startIndex));
                    expectedobjIDState.put("expectedobjIDState", smallSet);



                }
                //"syncState" , expectedBatchCount, totalBatchCount, currBatchCount, currBatchID, removeIDs));
                syncState.put("syncState", Tuple4.of(expectedBatchCount, totalBatchCount, currBatchCount, currBatchID));
                removeIDState.put("removeIDState", removeIDs);

                if (bcState.contains(edge1))
                {
                    currentTime =  System.currentTimeMillis();
                    totalEdgeTraffic1 = bcState.get(edge1).f0 + traffic1;
                    bcState.put(edge1, Tuple8.of(totalEdgeTraffic1, currentTime, 0, 0L, 0L, 0, 0L, 0L));
                }
                else {
                    currentTime =  System.currentTimeMillis();
                    bcState.put(edge1, Tuple8.of(traffic1, currentTime, 0, 0L, 0L, 0, 0L, 0L));
                }

                if (bcState.contains(edge2))
                {
                    currentTime =  System.currentTimeMillis();
                    totalEdgeTraffic2 = bcState.get(edge2).f0 + traffic2;
                    bcState.put(edge2, Tuple8.of(totalEdgeTraffic2, currentTime, 0, 0L, 0L, 0, 0L,0L));
                }
                else {
                    currentTime =  System.currentTimeMillis();
                    bcState.put(edge2, Tuple8.of(traffic2, currentTime, 0, 0L, 0L, 0, 0L,0L));
                }

                currentTime =  System.currentTimeMillis();
                deltaT = currentTime -  sideoutputTimestamp;

                //update_time, currenttime, seqID, size, deltaT, trajStatus,objID
                bcState.put("update_time", Tuple8.of(totalEdgeTraffic1, deltaT, totalEdgeTraffic2 , totalBatchCount, seqID, trajStatus, objID.longValue(), batchID));

            }

        });

        // "update_time", 0, 0L, 0 , bcCount, trajStatus, seqID, objID
        DataStream<Tuple9<String, Integer, String, Integer, Long, Long,Integer, Integer, Long>> edgeTrafficStream = networkPoints.getSideOutput(outputTag);
        DataStream<Tuple6<String, Long, Long, Long, Long, HashSet<Integer>>> controlTupleStream = networkPoints.getSideOutput(controlTupleTag);
        DataStream<String> edgeTrafficStreamSink = edgeTrafficStream.map(new MapFunction<Tuple9<String, Integer, String, Integer, Long, Long,Integer, Integer, Long>, String>() {
            @Override
            public String map(Tuple9<String, Integer, String, Integer, Long, Long,Integer, Integer, Long> edgeTrafficTuple) throws Exception {
                String edgeTrafficString = edgeTrafficTuple.f0 + "," + edgeTrafficTuple.f1 + "," +  edgeTrafficTuple.f2 + "," + edgeTrafficTuple.f3 + "," +  edgeTrafficTuple.f4 + "," + edgeTrafficTuple.f5 + "," + edgeTrafficTuple.f6 + "," + edgeTrafficTuple.f7 + "," + edgeTrafficTuple.f8;
                return edgeTrafficString;
            }
        });

        DataStream<String> controlTupleStreamSink = controlTupleStream.map(new MapFunction<Tuple6<String, Long, Long, Long, Long, HashSet<Integer>>,String>() {
            @Override
            public String map(Tuple6<String, Long, Long, Long, Long, HashSet<Integer>> controlTuple) throws Exception {
                String removeIDlistString = "0";
                if (controlTuple.f5.size() != 0)
                {removeIDlistString = controlTuple.f5.stream().map(Object::toString).collect(Collectors.joining("-"));}
                String controlTupleString = controlTuple.f0 + "," + controlTuple.f1 + "," +  controlTuple.f2 + "," + controlTuple.f3 + "," + controlTuple.f4 + "," + removeIDlistString;
                return controlTupleString;
            }
        });


//        edgeTrafficStreamSink.print();
        edgeTrafficStreamSink.addSink(new FlinkKafkaProducer<>("Feedback", new Serialization.StringToStringOutput("Feedback"), this.kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Traffic Information Sink");
        controlTupleStreamSink.addSink(new FlinkKafkaProducer<>("BroadcastStateUpdate", new Serialization.StringToStringOutput("BroadcastStateUpdate"), this.kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Control Tuple Sink");

        return networkPoints;
    }

}