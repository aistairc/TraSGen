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
package DataGen.utils;

import DataGen.inputParameters.Params;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.geotools.referencing.GeodeticCalculator;
import org.json.JSONObject;
import org.locationtech.jts.algorithm.ConvexHull;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.File;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;

import static DataGen.utils.Serialization.generatePolygonJson;
import static java.lang.System.exit;

public class GeoJSONMapToGraph implements Serializable {
    // TODO: convert nodeKey from string to Integer
    private final Map<String, Coordinate> nodeIDCoordinateMap = new HashMap<>();
    private final List<Tuple3<String, String, Map<String, String>>> edgeList = new ArrayList<>();
    private int nodeCounter = 0;
    private final String mapFile;
    private final String format;
    private final double tolerance;
    private List<Coordinate> Coordinates = new ArrayList<Coordinate>();  //
    private STRtree nodesSTRtree = new STRtree();
    private Polygon bufferedConvexHull = null;



    public GeoJSONMapToGraph(String mapFile, String format, double tolerance){
        this.mapFile = mapFile;
        this.format = format;
        this.tolerance = tolerance;
    }

    public int getNumVertices() {
        return nodeIDCoordinateMap.size();
    }

    public int getNumEdges() {
        return edgeList.size();
    }

    public STRtree getNodesSTRtree() {return nodesSTRtree;}
    public Polygon getBufferedConvexHull() {
        return bufferedConvexHull;
    }

    public void generateGraph(){

        List<Map.Entry<Map<String, String>, Geometry>> geoJsonFile = Deserialization.readGeoJsonFile(mapFile, format);

        File f = new File(mapFile);
        if(geoJsonFile != null){
            // Iterating through all the linestrings in a GeoJSON Map File
            for(Map.Entry<Map<String, String>, Geometry> mapPair: geoJsonFile){
                try {

                    List<String> lineStringNodesList = lineStringToListOfNodes((Geometry) mapPair.getValue(), tolerance);
                    Map<String, String> propertiesMap = mapPair.getKey();
                    GeodeticCalculator gc = new GeodeticCalculator(Params.coordinateReferenceSystem);

                    // Updating edgeList
                    for (int i = 0; i < lineStringNodesList.size() - 1; i++) {
                        Coordinate c1 = nodeIDCoordinateMap.get(lineStringNodesList.get(i));
                        Coordinate c2 = nodeIDCoordinateMap.get(lineStringNodesList.get(i + 1));
                        double edgeWeight = SpatialFunctions.getDistanceInMeters(c1, c2, Params.coordinateReferenceSystem, gc);
                        Map<String, String> edgePropertiesMap = new HashMap<>(propertiesMap);
                        // Adding computed Spatial Distance to edgePropertiesMap
                        edgePropertiesMap.put("weight", Double.toString(edgeWeight));
                        edgeList.add(new Tuple3<>(lineStringNodesList.get(i), lineStringNodesList.get(i + 1), edgePropertiesMap));
                    }

                }
                catch(Exception e){
                    if(e.toString().contains("Multi")){
                        System.out.println("SpatialDataGen supports GeoJSON maps consisting of LineStrings only.");
                    }else{
                        System.out.println(e);
                    }

                    exit(0);
                }
            }
        }
//        else
//        {
//            try {
//                Files.write(Paths.get("EmptyGeoJsonFile.txt"), "GeoJson Map File is Empty".getBytes());
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//
//
//        }



        Coordinate[] allPoints = new Coordinate[Coordinates.size()];
        this.Coordinates.toArray(allPoints);

        ConvexHull cbxBuidler = new ConvexHull(allPoints, new GeometryFactory());
        Geometry convexHull = cbxBuidler.getConvexHull();
        this.bufferedConvexHull = (Polygon) convexHull.buffer(0.05);

        // Output Convex Hull to GeoJson (for visualising in QGIS)
//        String dateTimeFormat = Params.dateFormat;
//        List<List<Coordinate>> nested = Arrays.asList(Arrays.asList(convexHull.getCoordinates()));
//        JSONObject json = generatePolygonJson(nested, 1,HelperClass.TimeStamp(Params.dateFormat, Params.initialTimeStamp, 0, 0L, null, false));



//        List<List<Coordinate>> nestedbuffer = Arrays.asList(Arrays.asList(bufferedConvexHull.getCoordinates()));
//        JSONObject jsonbuffer = generatePolygonJson(nestedbuffer, 1, simpleDateFormat.format(HelperClass.localDateTimeToDate(LocalDateTime.now())));
//        System.out.println("Convex Hull Buffer Polygon: ");
//        System.out.println(jsonbuffer.toString());



    }

    public Map<String, Coordinate> getNodeIDCoordinateMap(){
        return nodeIDCoordinateMap;
    }

    public Coordinate getNodeCoordinate(String nodeID){
        return nodeIDCoordinateMap.get(nodeID);
    }

    // Tuple3<String, String, Map<String, String>> --> edgeStartingNode, edgeEndingNode, properties map
    public List<Tuple3<String, String, Map<String, String>>> getEdgeList(){
        return edgeList;
    }

    public List<String> lineStringToListOfNodes(Geometry ls, double tolerance){

        List<String> lineStringNodesList = new ArrayList<>();

        // iterating through all the coordinates of a linestring
        for (Coordinate newCoordinate: ls.getCoordinates()){

            String nodeKey = null;
            Coordinate nodeCoordinate = null;

            Coordinates.add(newCoordinate);


            // Checking coordinates against existing nodes
            for (Map.Entry<String, Coordinate> entry : nodeIDCoordinateMap.entrySet()){
                if(newCoordinate.equals2D(entry.getValue(), tolerance)){

                    //Compute average using existing and new coordinates
                    double x = (newCoordinate.getX() + entry.getValue().getX())/2.0;
                    double y = (newCoordinate.getY() + entry.getValue().getY())/2.0;

                    // Updating the old entry in map
                    nodeKey = entry.getKey();
                    nodeCoordinate = new Coordinate(x,y);

                    // if node within tolerance found, break the loop
                    break;
                }
            }

            // if a node is not within tolerance, use new id and current coordinate value for this node
            if(nodeKey == null){

                nodeKey = Integer.toString(nodeCounter);
                nodeCoordinate = new Coordinate(newCoordinate.getX(), newCoordinate.getY());
                nodeCounter++;
            }

            nodeIDCoordinateMap.put(nodeKey, nodeCoordinate);
            lineStringNodesList.add(nodeKey);


            //insert node into STRTree
            Point nodePoint = new GeometryFactory().createPoint(newCoordinate);
            nodesSTRtree.insert(nodePoint.getEnvelopeInternal(), Tuple2.of(nodePoint,nodeKey));

        }

        return lineStringNodesList;
    }


}
