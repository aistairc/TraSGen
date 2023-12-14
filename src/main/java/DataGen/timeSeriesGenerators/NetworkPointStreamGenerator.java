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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
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

public class NetworkPointStreamGenerator implements StreamGenerator, Serializable {
    private String redisServerType;
    private String redisAddresses;
    private Map<Integer, GraphPath<String, DefaultWeightedEdge>> shortestIDPathMap = new HashMap<>();
    private NetworkPath networkPath = null;
    private final NetworkDistribution networkDistribution;
    private String outputFormat = "GeoJSON";

    private transient StreamExecutionEnvironment env = null;
    private Properties kafkaProperties = null;

    private CoordinateReferenceSystem crs;

    private final double displacementMetersPerSecond;

    String interWorkersDataSharing;


    private String initialTimeStamp = null;

    private String dateFormat = null;

    private int timeStepinMilliSec = 0;
    boolean randomizeTimeInBatch;

    public NetworkPointStreamGenerator(NetworkDistribution networkDistribution, String outputFormat, String mapFile, String mapFileFormat,
                                       String shortestPathAlgorithmStr, Double nodeMappingTolerance, int minObjID, int maxObjID,
                                       String trajStartEndSelectionApproach, List<List<Double>> trajStartEndCoordinatePairs,
                                       List<List<Double>> trajStartPolygons, List<List<Double>> trajEndPolygons, double displacementMetersPerSecond, CoordinateReferenceSystem crs, String interWorkersDataSharing,
                                       String redisAddresses, String redisServerType, String dateFormat, String initialTimeStamp, int timeStepinMilliSec, boolean randomizeTimeInBatch){

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
        this.interWorkersDataSharing = interWorkersDataSharing;
        this.redisAddresses = redisAddresses;
        this.redisServerType = redisServerType;
        this.initialTimeStamp = initialTimeStamp;
        this.timeStepinMilliSec = timeStepinMilliSec;
        this.dateFormat = dateFormat;
        this.randomizeTimeInBatch = randomizeTimeInBatch;

        if(this.interWorkersDataSharing.equalsIgnoreCase("redis")) {System.out.println("Redis interWorkersDataSharing option. Data sharing will be done using Redis.");}
    }


    @Override
    public DataStream<String> generate(DataStream<Tuple2<Integer, Long>> objIDStream) throws Exception {
        return objIDStream
                .keyBy(new HelperClass.objIDKeySelectorWithBatchID())
                .flatMap(new NetworkRichFlatMapFunction<Coordinate>(Coordinate.class, networkDistribution, this.shortestIDPathMap, this.crs, this.displacementMetersPerSecond, this.interWorkersDataSharing, this.redisAddresses, this.redisServerType) {

                    @Override
                    public void flatMap(Tuple2<Integer, Long> objIDTuple, Collector<String> collector) throws Exception {
                        Integer objID = objIDTuple.f0;
                        Long batchID = objIDTuple.f1;

                        //System.out.println("getNumberOfParallelSubtasks " + getRuntimeContext().getNumberOfParallelSubtasks());
                        //System.out.println("getIndexOfThisSubtask " + getRuntimeContext().getIndexOfThisSubtask());

                        LocalDateTime localDateTime = LocalDateTime.now();
                        GraphPath<String, DefaultWeightedEdge> shortestPath = this.shortestIDPathMap.get(objID);
                        List<DefaultWeightedEdge> shortestPathEdgeList = shortestPath.getEdgeList();

                        Integer currentEdgeIndex;
                        DefaultWeightedEdge currentEdge;
                        Coordinate outputPointCoordinates;
                        Coordinate lastPointCoordinates;
                        Double lastAzimuth;

                        seqID.update(seqID.value() + 1);

                        // If this is not the first edge of the path, retrieve edge using edge index
                        if (currentEdgeIndexVState != null && currentEdgeIndexVState.value() != null) {
                            currentEdgeIndex = currentEdgeIndexVState.value();
                            // If number of edges have exhausted, return nothing
                            if (currentEdgeIndex >= shortestPathEdgeList.size()) {
                                return;
                            } else {
                                currentEdge = shortestPathEdgeList.get(currentEdgeIndex);
                            }

                        } else { // Setting the initial value of currentEdgeIndex

                            currentEdgeIndex = 0;
                            currentEdge = shortestPathEdgeList.get(0);
                            currentEdgeIndexVState.update(currentEdgeIndex);
                            // Updating road segment obj count in redis
                            if(this.interWorkersDataSharing.equalsIgnoreCase("redis")) { redis.updateRoadSegmentsTrafficMap(currentEdge.toString(), 1);}
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
                            double currentDisplacementPerUnitTime = this.displacementMetersPerSecond;
                            if(this.interWorkersDataSharing.equalsIgnoreCase("redis")) {
                                currentDisplacementPerUnitTime = HelperClass.getDisplacementMetersPerSecond(ROAD_CAPACITY, edgeSourceCoordinates,edgeTargetCoordinates, redis.getRoadSegmentsTrafficMapValue(currentEdge.toString()), this.displacementMetersPerSecond, this.crs, this.gc);
                            }

                            if (remainingDistOnEdge <= currentDisplacementPerUnitTime){
                                outputPointCoordinates = edgeTargetCoordinates;
                                currentEdgeIndex++;
                                currentEdgeIndexVState.update(currentEdgeIndex);

                                // Return the currentEdge as the past edge if number of edges have exhausted
                                if (currentEdgeIndex >= shortestPathEdgeList.size()) {
                                    // Decrementing previous edge
                                    // currentEdge was the last edge which has been traversed, update its road segment obj count in redis
                                    if(this.interWorkersDataSharing.equalsIgnoreCase("redis")) { redis.updateRoadSegmentsTrafficMap(currentEdge.toString(), -1);}
                                    return;
                                } else { // edge(s) exist in the path
                                    // Decrement the previous edge in the redis
                                    if(this.interWorkersDataSharing.equalsIgnoreCase("redis")) { redis.updateRoadSegmentsTrafficMap(currentEdge.toString(), -1);}
                                    // fetch the next edge and set it to the currentEdge
                                    currentEdge = shortestPathEdgeList.get(currentEdgeIndex);
                                    // Increment the currentEdge traffic in redis
                                    if(this.interWorkersDataSharing.equalsIgnoreCase("redis")) { redis.updateRoadSegmentsTrafficMap(currentEdge.toString(), 1);}
                                }

                                // new edgeTarget as currentEdgeIndex has changed
                                edgeTarget = shortestPath.getGraph().getEdgeTarget(currentEdge).toString();
                                edgeTargetCoordinates = networkPath.getNodeCoordinate(edgeTarget);

                                Double edgeAzimuth = SpatialFunctions.getAzimuthInDecimalDegrees(outputPointCoordinates, edgeTargetCoordinates, this.crs, this .gc);

                                lastAzimuthVState.update(edgeAzimuth);
                                lastGeometryVState.update(outputPointCoordinates);
                            } else { // if remainingDistOnEdge > Params.displacementMetersPerSecond
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
                                if (dateFormat.equalsIgnoreCase("unix")) {
                                    collector.collect(Serialization.generatePointJson(
                                outputPointCoordinates.x, outputPointCoordinates.y, objID, seqID.value(), String.valueOf(System.currentTimeMillis())).toString());
                                }else {
                                    collector.collect(Serialization.generatePointJson(
                                            outputPointCoordinates.x, outputPointCoordinates.y, objID, seqID.value(), HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)).toString());
                                }

//                                collector.collect(Serialization.generatePointJson(
//                                        outputPointCoordinates.x, outputPointCoordinates.y, objID, seqID.value(), simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString());

                        } else {
                            collector.collect(Serialization.generateGeometryWKT(
                                    HelperClass.generatePoint(outputPointCoordinates), objID, seqID.value(), HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)));
                        }
                        // return for testing purpose
                        // return Serialization.generatePointJson( 10.0, 10.0, objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString();
                    }
                });

    }

//    @Override
//    public DataStream<String> generate2(DataStream<Tuple2<Integer, Long>> objIDStream, Envelope seriesBBox, SimpleDateFormat simpleDateFormat) throws Exception {
//        return null;
//    }
}