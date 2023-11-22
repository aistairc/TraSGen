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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.geojson.GeoJsonReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Deserialization {

    private static GeoJsonReader geoJsonReader = new GeoJsonReader(new GeometryFactory());

    public static List<Map.Entry<Map<String, String>, Geometry>> readGeoJsonFile(String path, String format) {
        try {
            String strJson = readFile(path);
            // GeoJSON
            if (format.equals("GeoJSON")) {
                return geoJsonToListGeometry(strJson);
            }
        }
        catch (SecurityException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    private static List<Map.Entry<Map<String, String>, Geometry>> geoJsonToListGeometry(String strJson) {
        try {
            List<Map.Entry<Map<String, String>, Geometry>> listMapPair = new ArrayList<>();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(strJson);
            JsonNode nodeFeatures = node.get("features");
            Iterator<JsonNode> iteratorFeatures = nodeFeatures.elements();

            while (iteratorFeatures.hasNext()) {
                JsonNode nodeFeature = iteratorFeatures.next();
                try {
                    Geometry geometry = readGeoJSON(nodeFeature.get("geometry").toString());
                    JsonNode nodeProperties = nodeFeature.get("properties");

                    Map map = new HashMap<>();
                    Iterator<String> iteratorNames = nodeProperties.fieldNames();

                    while (iteratorNames.hasNext()) {
                        String name = iteratorNames.next();

                        JsonNode nodeElement = nodeProperties.get(name);
                        if (nodeElement.getNodeType() == JsonNodeType.BOOLEAN) {
                            map.put(name, nodeElement.asBoolean());
                        }
                        else if (nodeElement.getNodeType() == JsonNodeType.NUMBER) {
                            if (nodeElement.asDouble() == nodeElement.asInt()) {
                                map.put(name, nodeElement.asInt());
                            }
                            else {
                                map.put(name, nodeElement.asDouble());
                            }
                        }
                        else if (nodeElement.getNodeType() == JsonNodeType.STRING) {
                            map.put(name, nodeElement.asText());
                        }
                    }

                    Map.Entry<Map<String,String>,Geometry> entry =
                            new AbstractMap.SimpleEntry<Map<String, String>,Geometry>(map, geometry);

                    listMapPair.add(entry);




                }
                // "geometry" が存在しないGeoJSON
                catch (Exception e) {
                    Geometry geometry = readGeoJSON(nodeFeature.toString());
                    Map map = new HashMap<>();
                    // propertiesは存在しないため、Mapも空となる
                    listMapPair.add( new AbstractMap.SimpleEntry<Map<String, String>,Geometry>(map, geometry));

                }
            }


            return listMapPair;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (SecurityException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    private static Geometry readGeoJSON(String geoJson) throws org.locationtech.jts.io.ParseException {
        return geoJsonReader.read(geoJson);
    }

    private static String readFile(String path) {
        BufferedReader br = null;
        FileReader fr = null;
        try {
            StringBuffer strBuf = new StringBuffer();
            fr = new FileReader(path);
            br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                strBuf.append(line);
            }
            br.close();
            fr.close();
            return strBuf.toString();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (br != null) {
                try {
                    br.close();
                }
                catch (IOException e) {}
            }
            if (fr != null) {
                try {
                    fr.close();
                }
                catch (IOException e) {}
            }
        }
        return null;
    }
}
