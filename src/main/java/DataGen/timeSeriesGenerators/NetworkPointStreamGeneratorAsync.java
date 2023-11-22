/*
 * Copyright 2023 Data Platform Research Team, AIRC, AIST, Japan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NetworkPointStreamGeneratorAsync implements StreamGenerator, Serializable {

    private Map<Integer, GraphPath<String, DefaultWeightedEdge>> shortestIDPathMap = new HashMap<>();
    private NetworkPath networkPath = null;
    private final NetworkDistribution networkDistribution;
    private String outputFormat = "GeoJSON";

    private transient StreamExecutionEnvironment env = null;
    private Properties kafkaProperties = null;

    private CoordinateReferenceSystem crs;
    private final double displacementMetersPerSecond;

    private String initialTimeStamp = null;

    private String dateFormat = null;

    private int timeStepinMilliSec = 0;
    boolean randomizeTimeInBatch;


    public NetworkPointStreamGeneratorAsync(NetworkDistribution networkDistribution, Properties kafkaProperties, StreamExecutionEnvironment env, String outputFormat,
                                            String mapFile, String mapFileFormat, String shortestPathAlgorithmStr, Double nodeMappingTolerance,
                                            int minObjID, int maxObjID, String trajStartEndSelectionApproach, List<List<Double>> trajStartEndCoordinatePairs,
                                            List<List<Double>> trajStartPolygons, List<List<Double>> trajEndPolygons, double displacementMetersPerSecond, CoordinateReferenceSystem crs,
                                            String dateFormat, String initialTimeStamp, int timeStepinMilliSec, boolean randomizeTimeInBatch){

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
        this.initialTimeStamp = initialTimeStamp;
        this.timeStepinMilliSec = timeStepinMilliSec;
        this.dateFormat = dateFormat;
        this.randomizeTimeInBatch = randomizeTimeInBatch;
    }


    @Override
    public DataStream<String> generate(DataStream<Tuple2<Integer, Long>> objIDStream, SimpleDateFormat simpleDateFormat) {

        //read edge information from Kafka topic Feedback
        DataStream<String> edgeTrafficCountString = this.env.addSource(new FlinkKafkaConsumer<>("Feedback", new SimpleStringSchema(), this.kafkaProperties)).name("Traffic Information Source");
        //Deserialize String -> Tuple2
        DataStream<Tuple4<String, Integer, String,Integer>> edgeTrafficCount = edgeTrafficCountString.map(new MapFunction<String, Tuple4<String, Integer, String,Integer>>() {
            @Override
            public Tuple4<String, Integer, String, Integer> map(String str) throws Exception {
                String[] temp = str.split(",");
                return Tuple4.of(temp[0], Integer.parseInt(temp[1]),temp[2], Integer.parseInt(temp[3]));
            }
        });

        KeyedStream<Tuple2<Integer,Long>, Integer> keyedobjIDStream = objIDStream.keyBy(new HelperClass.objIDKeySelectorWithBatchID());

        MapStateDescriptor<String,Tuple3<Integer, String, Integer>> broadcastStateDescriptor =
                new MapStateDescriptor<>("edgeTrafficMap", BasicTypeInfo.STRING_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, String.class,Integer.class));

        BroadcastStream<Tuple4<String, Integer, String, Integer>> broadcastTrafficMap = edgeTrafficCount.broadcast(broadcastStateDescriptor); //Broadcast edgeTrafficCount
        //OutputTag required to generate side output datastream sent to be written to Feedback
        final OutputTag<Tuple4<String, Integer, String, Integer>> outputTag = new OutputTag<Tuple4<String, Integer, String, Integer>>("feedback-sideoutput"){};
        final OutputTag<Tuple4<String, Integer, String, Integer>> broadcastStateTag = new OutputTag<Tuple4<String, Integer, String, Integer>>("broadcaststate-sideoutput"){};

        SingleOutputStreamOperator<String> networkPoints = keyedobjIDStream.connect(broadcastTrafficMap).process(new NetworkBroadcastProcessFunction<Coordinate>(Coordinate.class, networkDistribution, this.shortestIDPathMap, this.crs, this.displacementMetersPerSecond) {

            @Override
            public void processElement(Tuple2<Integer, Long> objIDTuple,  ReadOnlyContext ctx, Collector<String> collector) throws Exception {

                Integer objID = objIDTuple.f0;
                Long batchID = objIDTuple.f1;

                LocalDateTime localDateTime = LocalDateTime.now();

                GraphPath<String, DefaultWeightedEdge> shortestPath = this.shortestIDPathMap.get(objID);
                List<DefaultWeightedEdge> shortestPathEdgeList = shortestPath.getEdgeList();

                Integer currentEdgeIndex;
                DefaultWeightedEdge currentEdge;
                Coordinate outputPointCoordinates;
                Coordinate lastPointCoordinates;
                Double lastAzimuth;
                Integer currentRoadTraffic = 0;
                Double currentDisplacementPerUnitTime =  0.0;

                ReadOnlyBroadcastState<String,Tuple3<Integer, String, Integer>> bcState = ctx.getBroadcastState(this.edgeTrafficMapDesc);

                seqID.update(seqID.value() + 1);

                // Retrieving edge using edge index
                if (currentEdgeIndexVState != null && currentEdgeIndexVState.value() != null) {

                    currentEdgeIndex = currentEdgeIndexVState.value();

                    if (currentEdgeIndex >= shortestPathEdgeList.size()) {
                        return;
                    } else {
                        currentEdge = shortestPathEdgeList.get(currentEdgeIndex);
                    }
                    //currentEdge = shortestPathEdgeList.get(currentEdgeIndex);

                } else { // Setting the initial value of currentEdgeIndex

                    currentEdgeIndex = 0;
                    currentEdge = shortestPathEdgeList.get(0);
                    currentEdgeIndexVState.update(currentEdgeIndex);

                    ctx.output(outputTag, Tuple4.of(currentEdge.toString(), 1,  String.valueOf(System.currentTimeMillis()), seqID.value().intValue() ));      //Traffic + 1 on this edge

                }

                // If one or more trajectory tuples already generated
                if (lastGeometryVState != null && lastGeometryVState.value() != null && lastAzimuthVState != null && lastAzimuthVState.value() != null) {

                    lastPointCoordinates = this.lastGeometryVState.value();
                    lastAzimuth = this.lastAzimuthVState.value();

                    String edgeTarget = shortestPath.getGraph().getEdgeTarget(currentEdge);
                    Coordinate edgeTargetCoordinates = networkPath.getNodeCoordinate(edgeTarget);
                    String edgeSource =  shortestPath.getGraph().getEdgeSource(currentEdge);
                    Coordinate edgeSourceCoordinates = networkPath.getNodeCoordinate(edgeSource);

                    // If the remaining distance on edge is less than NETWORK_DISPLACEMENT_METERS, return the edge target coordinate
                    double remainingDistOnEdge = SpatialFunctions.getDistanceInMeters(lastPointCoordinates, edgeTargetCoordinates, this.crs, this.gc);

                    //System.out.println("remainingDistOnEdge " + remainingDistOnEdge);
                    if (bcState.get(currentEdge.toString()) != null ) {
                        currentRoadTraffic = bcState.get(currentEdge.toString()).f0;
                    }

                    currentDisplacementPerUnitTime = HelperClass.getDisplacementMetersPerSecond(ROAD_CAPACITY, edgeSourceCoordinates,edgeTargetCoordinates, currentRoadTraffic, this.displacementMetersPerSecond, this.crs, this.gc);
//                    if (currentDisplacementPerUnitTime != 16.0) {System.out.println(currentDisplacementPerUnitTime);}
                    if (remainingDistOnEdge <= currentDisplacementPerUnitTime){
                        outputPointCoordinates = edgeTargetCoordinates;
                        currentEdgeIndex++;
                        currentEdgeIndexVState.update(currentEdgeIndex);

                        // Return the currentEdge as the past edge if number of edges have exhausted
                        if (currentEdgeIndex >= shortestPathEdgeList.size()) {

                            ctx.output(outputTag, Tuple4.of(currentEdge.toString(), -1,  String.valueOf(System.currentTimeMillis()), seqID.value().intValue()));
                            return;
                        } else {
                            ctx.output(outputTag, Tuple4.of(currentEdge.toString(), -1,  String.valueOf(System.currentTimeMillis()), seqID.value().intValue())); //Traffic - 1 on previous edge
                            currentEdge = shortestPathEdgeList.get(currentEdgeIndex);
                            ctx.output(outputTag, Tuple4.of(currentEdge.toString(), 1,  String.valueOf(System.currentTimeMillis()), seqID.value().intValue())); //Traffic + 1 on current edge

                        }

                        // new edgeTarget as currentEdgeIndex has changed
                        edgeTarget = shortestPath.getGraph().getEdgeTarget(currentEdge).toString();
                        edgeTargetCoordinates = networkPath.getNodeCoordinate(edgeTarget);

                        Double edgeAzimuth = SpatialFunctions.getAzimuthInDecimalDegrees(outputPointCoordinates, edgeTargetCoordinates, this.crs, this.gc);

                        lastAzimuthVState.update(edgeAzimuth);
                        lastGeometryVState.update(outputPointCoordinates);

                    } else {
                        outputPointCoordinates = networkDistribution.next(lastPointCoordinates, lastAzimuth, currentDisplacementPerUnitTime, this.crs, this.gc);
                        lastGeometryVState.update(outputPointCoordinates);
                    }

                } else { // If this is the first trajecctory tuple

                    String edgeSource = shortestPath.getGraph().getEdgeSource(currentEdge).toString();
                    Coordinate edgeSourceCoordinates = networkPath.getNodeCoordinate(edgeSource);

                    String edgeTarget = shortestPath.getGraph().getEdgeTarget(currentEdge).toString();
                    Coordinate edgeTargetCoordinates = networkPath.getNodeCoordinate(edgeTarget);

                    Double edgeAzimuth = SpatialFunctions.getAzimuthInDecimalDegrees(edgeSourceCoordinates, edgeTargetCoordinates, this.crs, this.gc);

                    outputPointCoordinates = edgeSourceCoordinates;

                    lastGeometryVState.update(edgeSourceCoordinates);
                    lastAzimuthVState.update(edgeAzimuth);
                }

                if (outputFormat.equals("GeoJSON")) {
//                    collector.collect(Serialization.generatePointJson(
//                            outputPointCoordinates.x, outputPointCoordinates.y, objID, seqID.value(), currentEdge.toString().replaceAll("[\\p{Ps}\\p{Pe}]", ""), currentRoadTraffic, currentDisplacementPerUnitTime,  simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString());
                        collector.collect(Serialization.generatePointJson(
                                outputPointCoordinates.x, outputPointCoordinates.y, objID, seqID.value(), currentEdge.toString().replaceAll("[\\p{Ps}\\p{Pe}]", ""), currentRoadTraffic, currentDisplacementPerUnitTime, HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)).toString());
//                        collector.collect(Serialization.generatePointJson(
//                                outputPointCoordinates.x, outputPointCoordinates.y, objID, seqID.value(), currentEdge.toString().replaceAll("[\\p{Ps}\\p{Pe}]", ""), currentRoadTraffic, currentDisplacementPerUnitTime, String.valueOf(System.currentTimeMillis())).toString());

                } else {
                    collector.collect(Serialization.generateGeometryWKT(
                            HelperClass.generatePoint(outputPointCoordinates), objID, seqID.value(), simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))));
                }

            }

            @Override
            public void processBroadcastElement(Tuple4<String, Integer, String, Integer> edgeTraffic, Context ctx, Collector<String> collector) throws Exception {

                BroadcastState<String,Tuple3<Integer, String, Integer>> bcState = ctx.getBroadcastState(this.edgeTrafficMapDesc);

                if (bcState.contains(edgeTraffic.f0))
                {
                    Integer totalEdgeTraffic = bcState.get(edgeTraffic.f0).f0 + edgeTraffic.f1;
                    bcState.put(edgeTraffic.f0, Tuple3.of(totalEdgeTraffic, String.valueOf(System.currentTimeMillis()), edgeTraffic.f3));
                }
                else {
                    bcState.put(edgeTraffic.f0, Tuple3.of(edgeTraffic.f1, String.valueOf(System.currentTimeMillis()), edgeTraffic.f3));
                }

                ctx.output(broadcastStateTag, Tuple4.of(edgeTraffic.f0 , bcState.get(edgeTraffic.f0).f0, bcState.get(edgeTraffic.f0).f1, bcState.get(edgeTraffic.f0).f2 ));
            }

        });
        DataStream<Tuple4<String, Integer, String, Integer>> edgeTrafficStream = networkPoints.getSideOutput(outputTag);
        DataStream<Tuple4<String, Integer, String, Integer>> bcStateStream = networkPoints.getSideOutput(broadcastStateTag);
        DataStream<String> edgeTrafficStreamSink = edgeTrafficStream.map(new MapFunction<Tuple4<String, Integer, String, Integer>, String>() {
            @Override
            public String map(Tuple4<String, Integer, String, Integer> edgeTrafficTuple) throws Exception {
                String edgeTrafficString = edgeTrafficTuple.f0 + "," + edgeTrafficTuple.f1.toString() + "," +  edgeTrafficTuple.f2 + "," + edgeTrafficTuple.f3 ;
                return edgeTrafficString;
            }
        });


        DataStream<String> bcStateStreamSink = bcStateStream.map(new MapFunction<Tuple4<String, Integer, String, Integer>, String>() {
            @Override
            public String map(Tuple4<String, Integer, String, Integer> edgeTrafficTuple) throws Exception {
                String edgeTrafficString = edgeTrafficTuple.f0 + "," + edgeTrafficTuple.f1.toString() + "," +  edgeTrafficTuple.f2 + "," + edgeTrafficTuple.f3 ;
                return edgeTrafficString;
            }
        });


//        edgeTrafficStreamSink.print();
        edgeTrafficStreamSink.addSink(new FlinkKafkaProducer<>("Feedback", new Serialization.StringToStringOutput("Feedback"), this.kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Traffic Update Sink");
        bcStateStreamSink.addSink(new FlinkKafkaProducer<>("BroadcastStateUpdate", new Serialization.StringToStringOutput("BroadcastStateUpdate"), this.kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Control Tuple Sink");

        //        edgeTrafficStreamSink.addSink(new ZMQSink<String>(config, new SimpleStringSchema()));
//        edgeTrafficCount.print();

        return networkPoints;
    }

}