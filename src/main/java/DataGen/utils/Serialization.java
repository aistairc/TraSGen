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

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.locationtech.jts.geom.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Serialization {

    public static class StringToStringOutput implements Serializable, KafkaSerializationSchema<String> {

        private String outputTopic;

        public StringToStringOutput(String outputTopicName) {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String inputElement, @Nullable Long timestamp) {

            return new ProducerRecord<byte[], byte[]>(outputTopic, inputElement.getBytes(StandardCharsets.UTF_8));
        }
    }

    // Generate JSON Objects

    public static JSONObject generatePointJson(double x, double y, int objID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        double[] coordinate = {x, y};
        jsonGeometry.put("coordinates", coordinate);
        jsonGeometry.put("type", "Point");
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generatePointJson(double x, double y, int objID, long seqID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        double[] coordinate = {x, y};
        jsonGeometry.put("coordinates", coordinate);
        jsonGeometry.put("type", "Point");
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("seqID", String.valueOf(seqID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generatePointJson(double x, double y, int objID, long seqID, String currentEdge, Integer currentRoadTraffic, Double currentDisplacementPerUnitTime ,String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        double[] coordinate = {x, y};
        jsonGeometry.put("coordinates", coordinate);
        jsonGeometry.put("type", "Point");
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("seqID", String.valueOf(seqID));
        jsonpProperties.put("currentEdge", String.valueOf(currentEdge));
        jsonpProperties.put("currentRoadTraffic", String.valueOf(currentRoadTraffic));
        jsonpProperties.put("currentDisplacementPerUnitTime", String.valueOf(currentDisplacementPerUnitTime));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generatePolygonJson(List<List<Coordinate>> nestedCoordinateList, int objID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<List<double[]>> jsonCoordinate = new ArrayList<List<double[]>>();
        for (List<Coordinate> polygonCoordinates : nestedCoordinateList) {
            List<double[]> coordinates = new ArrayList<double[]>();
            for (Coordinate c : polygonCoordinates) {
                double[] coordinate = {c.x, c.y};
                coordinates.add(coordinate);
            }
            jsonCoordinate.add(coordinates);
        }
        jsonGeometry.put("type", "Polygon");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generatePolygonJson(List<List<Coordinate>> nestedCoordinateList, int objID, long seqID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<List<double[]>> jsonCoordinate = new ArrayList<List<double[]>>();
        for (List<Coordinate> polygonCoordinates : nestedCoordinateList) {
            List<double[]> coordinates = new ArrayList<double[]>();
            for (Coordinate c : polygonCoordinates) {
                double[] coordinate = {c.x, c.y};
                coordinates.add(coordinate);
            }
            jsonCoordinate.add(coordinates);
        }
        jsonGeometry.put("type", "Polygon");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("seqID", String.valueOf(seqID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generateMultiPolygonJson(List<List<List<Coordinate>>> nestedCoordinateList, int objID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<List<List<double[]>>> jsonCoordinate = new ArrayList<List<List<double[]>>>();
        for (List<List<Coordinate>> listCoordinate : nestedCoordinateList) {
            List<List<double[]>> coordinates = new ArrayList<>();
            for (List<Coordinate> l : listCoordinate) {
                List<double[]> arrCoordinate = new ArrayList<double[]>();
                for (Coordinate c : l) {
                    double[] coordinate = {c.x, c.y};
                    arrCoordinate.add(coordinate);
                }
                coordinates.add(arrCoordinate);
            }
            jsonCoordinate.add(coordinates);
        }
        jsonGeometry.put("type", "MultiPolygon");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generateMultiPolygonJson(List<List<List<Coordinate>>> nestedCoordinateList, int objID, long seqID,String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<List<List<double[]>>> jsonCoordinate = new ArrayList<List<List<double[]>>>();
        for (List<List<Coordinate>> listCoordinate : nestedCoordinateList) {
            List<List<double[]>> coordinates = new ArrayList<>();
            for (List<Coordinate> l : listCoordinate) {
                List<double[]> arrCoordinate = new ArrayList<double[]>();
                for (Coordinate c : l) {
                    double[] coordinate = {c.x, c.y};
                    arrCoordinate.add(coordinate);
                }
                coordinates.add(arrCoordinate);
            }
            jsonCoordinate.add(coordinates);
        }
        jsonGeometry.put("type", "MultiPolygon");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("seqID", String.valueOf(seqID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generateLineStringJson(List<Coordinate> nestedCoordinateList, int objID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<double[]> jsonCoordinate = new ArrayList<double[]>();
        for (Coordinate c : nestedCoordinateList) {
            double[] coordinate = {c.x, c.y};
            jsonCoordinate.add(coordinate);
        }
        jsonGeometry.put("type", "LineString");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }
    public static JSONObject generateLineStringJson(List<Coordinate> nestedCoordinateList, int objID, long seqID ,String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<double[]> jsonCoordinate = new ArrayList<double[]>();
        for (Coordinate c : nestedCoordinateList) {
            double[] coordinate = {c.x, c.y};
            jsonCoordinate.add(coordinate);
        }
        jsonGeometry.put("type", "LineString");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("seqID", String.valueOf(seqID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generateMultiLineStringJson(List<List<Coordinate>> nestedCoordinateList, int objID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<List<double[]>> jsonCoordinate = new ArrayList<>();
        for (List<Coordinate> l : nestedCoordinateList) {
            List<double[]> arrCoordinate = new ArrayList<>();
            for (Coordinate c : l) {
                double[] coordinate = {c.x, c.y};
                arrCoordinate.add(coordinate);
            }
            jsonCoordinate.add(arrCoordinate);
        }
        jsonGeometry.put("type", "MultiLineString");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generateMultiLineStringJson(List<List<Coordinate>> nestedCoordinateList, int objID, long seqID, String dateTimeString) {
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        List<List<double[]>> jsonCoordinate = new ArrayList<>();
        for (List<Coordinate> l : nestedCoordinateList) {
            List<double[]> arrCoordinate = new ArrayList<>();
            for (Coordinate c : l) {
                double[] coordinate = {c.x, c.y};
                arrCoordinate.add(coordinate);
            }
            jsonCoordinate.add(arrCoordinate);
        }
        jsonGeometry.put("type", "MultiLineString");
        jsonGeometry.put("coordinates", jsonCoordinate);
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("seqID", String.valueOf(seqID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }


    public static JSONObject generateGeometryJson(Geometry geometry, int objID, String dateTimeString){
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        if (Objects.equals(geometry.getGeometryType(), "Point")) {
            Point point = (Point)geometry;
            double[] coordinate = {point.getX(), point.getY()};
            jsonGeometry.put("type", "Point");
            jsonGeometry.put("coordinates", coordinate);
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPoint")) {
            MultiPoint multiPoint = (MultiPoint)geometry;
            Coordinate[] multiPointCoordinates = multiPoint.getCoordinates();
            List<double[]> jsonCoordinate = new ArrayList<>();
            for (Coordinate c : multiPointCoordinates) {
                double[] coordinate = {c.x, c.y};
                jsonCoordinate.add(coordinate);
            }
            jsonGeometry.put("type", "MultiPoint");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if (Objects.equals(geometry.getGeometryType(), "Polygon")){
            Polygon polygon = (Polygon)geometry;
            List<List<double[]>> jsonCoordinate = new ArrayList<>();

            List<List<Coordinate>> listlistCoordinates = new ArrayList();
            listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getExteriorRing().getCoordinates())));
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getInteriorRingN(i).getCoordinates())));
            }

            for (List<Coordinate> polygonCoordinates : listlistCoordinates) {
                List<double[]> coordinates = new ArrayList<>();
                for (Coordinate c : polygonCoordinates) {
                    double[] coordinate = {c.x, c.y};
                    coordinates.add(coordinate);
                }
                jsonCoordinate.add(coordinates);
            }
            jsonGeometry.put("type", "Polygon");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPolygon")) {
            MultiPolygon multiPolygon = (MultiPolygon)geometry;
            List<List<List<double[]>>> jsonCoordinate = new ArrayList<>();
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                Polygon polygon = (Polygon)multiPolygon.getGeometryN(i);

                List<List<Coordinate>> listlistCoordinates = new ArrayList();
                listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getExteriorRing().getCoordinates())));
                for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
                    listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getInteriorRingN(j).getCoordinates())));
                }

                List<List<double[]>> coordinates = new ArrayList<>();
                for (List<Coordinate> l : listlistCoordinates) {
                    List<double[]> arrCoordinate = new ArrayList<>();
                    for (Coordinate c : l) {
                        double[] coordinate = {c.x, c.y};
                        arrCoordinate.add(coordinate);
                    }
                    coordinates.add(arrCoordinate);
                }
                jsonCoordinate.add(coordinates);
            }
            jsonGeometry.put("type", "MultiPolygon");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if(Objects.equals(geometry.getGeometryType(), "LineString")){
            LineString lineString = (LineString)geometry;
            Coordinate[] lineStringCoordinates = lineString.getCoordinates();
            List<double[]> jsonCoordinate = new ArrayList<>();
            for (Coordinate c : lineStringCoordinates) {
                double[] coordinate = {c.x, c.y};
                jsonCoordinate.add(coordinate);
            }
            jsonGeometry.put("type", "LineString");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if(Objects.equals(geometry.getGeometryType(), "MultiLineString")) {
            MultiLineString multiLineString = (MultiLineString)geometry;
            List<List<double[]>> jsonCoordinate = new ArrayList<>();
            for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
                LineString lineString = (LineString)multiLineString.getGeometryN(i);
                Coordinate[] lineStringCoordinates = lineString.getCoordinates();
                List<double[]> listCoordinate = new ArrayList<>();
                for (Coordinate c : lineStringCoordinates) {
                    double[] coordinate = {c.x, c.y};
                    listCoordinate.add(coordinate);
                }
                jsonCoordinate.add(listCoordinate);
            }
            jsonGeometry.put("type", "MultiLineString");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    public static JSONObject generateGeometryJson(Geometry geometry, int objID, long seqID , String dateTimeString){
        JSONObject jsonObj = new JSONObject();

        JSONObject jsonGeometry = new JSONObject();
        if (Objects.equals(geometry.getGeometryType(), "Point")) {
            Point point = (Point)geometry;
            double[] coordinate = {point.getX(), point.getY()};
            jsonGeometry.put("type", "Point");
            jsonGeometry.put("coordinates", coordinate);
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPoint")) {
            MultiPoint multiPoint = (MultiPoint)geometry;
            Coordinate[] multiPointCoordinates = multiPoint.getCoordinates();
            List<double[]> jsonCoordinate = new ArrayList<>();
            for (Coordinate c : multiPointCoordinates) {
                double[] coordinate = {c.x, c.y};
                jsonCoordinate.add(coordinate);
            }
            jsonGeometry.put("type", "MultiPoint");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if (Objects.equals(geometry.getGeometryType(), "Polygon")){
            Polygon polygon = (Polygon)geometry;
            List<List<double[]>> jsonCoordinate = new ArrayList<>();

            List<List<Coordinate>> listlistCoordinates = new ArrayList();
            listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getExteriorRing().getCoordinates())));
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getInteriorRingN(i).getCoordinates())));
            }

            for (List<Coordinate> polygonCoordinates : listlistCoordinates) {
                List<double[]> coordinates = new ArrayList<>();
                for (Coordinate c : polygonCoordinates) {
                    double[] coordinate = {c.x, c.y};
                    coordinates.add(coordinate);
                }
                jsonCoordinate.add(coordinates);
            }
            jsonGeometry.put("type", "Polygon");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPolygon")) {
            MultiPolygon multiPolygon = (MultiPolygon)geometry;
            List<List<List<double[]>>> jsonCoordinate = new ArrayList<>();
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                Polygon polygon = (Polygon)multiPolygon.getGeometryN(i);

                List<List<Coordinate>> listlistCoordinates = new ArrayList();
                listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getExteriorRing().getCoordinates())));
                for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
                    listlistCoordinates.add(new ArrayList<>(Arrays.asList(polygon.getInteriorRingN(j).getCoordinates())));
                }

                List<List<double[]>> coordinates = new ArrayList<>();
                for (List<Coordinate> l : listlistCoordinates) {
                    List<double[]> arrCoordinate = new ArrayList<>();
                    for (Coordinate c : l) {
                        double[] coordinate = {c.x, c.y};
                        arrCoordinate.add(coordinate);
                    }
                    coordinates.add(arrCoordinate);
                }
                jsonCoordinate.add(coordinates);
            }
            jsonGeometry.put("type", "MultiPolygon");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if(Objects.equals(geometry.getGeometryType(), "LineString")){
            LineString lineString = (LineString)geometry;
            Coordinate[] lineStringCoordinates = lineString.getCoordinates();
            List<double[]> jsonCoordinate = new ArrayList<>();
            for (Coordinate c : lineStringCoordinates) {
                double[] coordinate = {c.x, c.y};
                jsonCoordinate.add(coordinate);
            }
            jsonGeometry.put("type", "LineString");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        else if(Objects.equals(geometry.getGeometryType(), "MultiLineString")) {
            MultiLineString multiLineString = (MultiLineString)geometry;
            List<List<double[]>> jsonCoordinate = new ArrayList<>();
            for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
                LineString lineString = (LineString)multiLineString.getGeometryN(i);
                Coordinate[] lineStringCoordinates = lineString.getCoordinates();
                List<double[]> listCoordinate = new ArrayList<>();
                for (Coordinate c : lineStringCoordinates) {
                    double[] coordinate = {c.x, c.y};
                    listCoordinate.add(coordinate);
                }
                jsonCoordinate.add(listCoordinate);
            }
            jsonGeometry.put("type", "MultiLineString");
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        jsonpProperties.put("oID", String.valueOf(objID));
        jsonpProperties.put("seqID", String.valueOf(seqID));
        jsonpProperties.put("timestamp", dateTimeString);
        jsonObj.put("properties", jsonpProperties);

        jsonObj.put("type", "Feature");
        return jsonObj;
    }

    // Generate WKT Objects
    public static String generateGeometryWKT(Geometry geometry, int objID, String dateTimeString) {
        final String SEPARATION = ",";
        StringBuffer buf = new StringBuffer();

        buf.append(objID);
        buf.append(SEPARATION + " ");
        if (Objects.equals(geometry.getGeometryType(), "Point")) {
            Point point = (Point)geometry;
            buf.append("POINT(");
            buf.append(point.getX());
            buf.append(" ");
            buf.append(point.getY());
            buf.append(")");
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPoint")) {
            MultiPoint multiPoint = (MultiPoint)geometry;
            buf.append("MULTIPOINT");
            buf.append("(");
            Coordinate[] coordinates = multiPoint.getCoordinates();
            for (Coordinate c : coordinates) {
                buf.append("(" + c.x + " " + c.y + "), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if (Objects.equals(geometry.getGeometryType(), "Polygon")){
            Polygon polygon = (Polygon)geometry;
            buf.append("POLYGON");
            buf.append("(");
            LineString exteriorRing = polygon.getExteriorRing();
            buf.append("(");
            for (Coordinate c : exteriorRing.getCoordinates()) {
                buf.append(c.x + " " + c.y + ", ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append("), ");
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                buf.append("(");
                for (Coordinate c : polygon.getInteriorRingN(i).getCoordinates()) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPolygon")) {
            MultiPolygon multiPolygon = (MultiPolygon)geometry;
            buf.append("MULTIPOLYGON");
            buf.append("(");
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                buf.append("(");
                Polygon polygon = (Polygon)multiPolygon.getGeometryN(i);
                LineString exteriorRing = polygon.getExteriorRing();
                buf.append("(");
                for (Coordinate c : exteriorRing.getCoordinates()) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
                for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
                    buf.append("(");
                    for (Coordinate c : polygon.getInteriorRingN(j).getCoordinates()) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append("), ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if(Objects.equals(geometry.getGeometryType(), "LineString")){
            LineString lineString = (LineString)geometry;
            buf.append("LINESTRING");
            buf.append("(");
            Coordinate[] coordinates = lineString.getCoordinates();
            for (Coordinate c : coordinates) {
                buf.append(c.x + " " + c.y + ", ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if(Objects.equals(geometry.getGeometryType(), "MultiLineString")) {
            MultiLineString multiLineString = (MultiLineString)geometry;
            buf.append("MULTILINESTRING");
            buf.append("(");
            for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
                buf.append("(");
                LineString lineString = (LineString)multiLineString.getGeometryN(i);
                for (Coordinate c : lineString.getCoordinates()) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }

        if (dateTimeString != null) {
            buf.append(SEPARATION + " ");
            buf.append(dateTimeString);
        }
        return buf.toString();
    }

    public static String generateGeometryWKT(Geometry geometry, int objID, long seqID , String dateTimeString) {
        final String SEPARATION = ",";
        StringBuffer buf = new StringBuffer();

        buf.append(objID);
        buf.append(SEPARATION + " ");
        buf.append(seqID);
        buf.append(SEPARATION + " ");
        if (Objects.equals(geometry.getGeometryType(), "Point")) {
            Point point = (Point)geometry;
            buf.append("POINT(");
            buf.append(point.getX());
            buf.append(" ");
            buf.append(point.getY());
            buf.append(")");
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPoint")) {
            MultiPoint multiPoint = (MultiPoint)geometry;
            buf.append("MULTIPOINT");
            buf.append("(");
            Coordinate[] coordinates = multiPoint.getCoordinates();
            for (Coordinate c : coordinates) {
                buf.append("(" + c.x + " " + c.y + "), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if (Objects.equals(geometry.getGeometryType(), "Polygon")){
            Polygon polygon = (Polygon)geometry;
            buf.append("POLYGON");
            buf.append("(");
            LineString exteriorRing = polygon.getExteriorRing();
            buf.append("(");
            for (Coordinate c : exteriorRing.getCoordinates()) {
                buf.append(c.x + " " + c.y + ", ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append("), ");
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                buf.append("(");
                for (Coordinate c : polygon.getInteriorRingN(i).getCoordinates()) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if (Objects.equals(geometry.getGeometryType(), "MultiPolygon")) {
            MultiPolygon multiPolygon = (MultiPolygon)geometry;
            buf.append("MULTIPOLYGON");
            buf.append("(");
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                buf.append("(");
                Polygon polygon = (Polygon)multiPolygon.getGeometryN(i);
                LineString exteriorRing = polygon.getExteriorRing();
                buf.append("(");
                for (Coordinate c : exteriorRing.getCoordinates()) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
                for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
                    buf.append("(");
                    for (Coordinate c : polygon.getInteriorRingN(j).getCoordinates()) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append("), ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if(Objects.equals(geometry.getGeometryType(), "LineString")){
            LineString lineString = (LineString)geometry;
            buf.append("LINESTRING");
            buf.append("(");
            Coordinate[] coordinates = lineString.getCoordinates();
            for (Coordinate c : coordinates) {
                buf.append(c.x + " " + c.y + ", ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        else if(Objects.equals(geometry.getGeometryType(), "MultiLineString")) {
            MultiLineString multiLineString = (MultiLineString)geometry;
            buf.append("MULTILINESTRING");
            buf.append("(");
            for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
                buf.append("(");
                LineString lineString = (LineString)multiLineString.getGeometryN(i);
                for (Coordinate c : lineString.getCoordinates()) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append("), ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }

        if (dateTimeString != null) {
            buf.append(SEPARATION + " ");
            buf.append(dateTimeString);
        }
        return buf.toString();
    }
}

