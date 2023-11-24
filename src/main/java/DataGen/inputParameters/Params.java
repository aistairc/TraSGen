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

package DataGen.inputParameters;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.constructor.Constructor;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Params {

    /**
     * Config File
     */
    public final String YAML_CONFIG = "spatialdatagen-conf.yml";
    public final String YAML_PATH = new File(".").getAbsoluteFile().getParent() + File.separator +
            "conf" + File.separator + YAML_CONFIG;

    /**
     * Parameters
     */
    /* Cluster */
    public static boolean clusterMode;

    /* Parallelism */
    public static int parallelism = 1;

    /* output */
    public static String outputOption;
    public static String outputFormat;

    /* output - kafka */
    public static String outputTopicName;
    public static String bootStrapServers;

    /* output - file */
    private static final String DEFAULT_OUTPUT_DIR_NAME = "data/output/";
    public static String outputDirName;
    public static long outputRollingRolloverInterval = 0L;
    public static long outputRollingInactivityInterval = 0L;
    public static long outputRollingMaxPartSize = 0L;

    /* output - data */
    public static String dateFormat;
    public static String initialTimeStamp;
    public static int timeStep;
    public static List<Integer> objIDRange;
    public static long nRows;
    public static int consecutiveTrajTuplesIntervalMilliSec;


    /* query */
    public static CoordinateReferenceSystem coordinateReferenceSystem;


    /* mappedTrajectories */
    public static String mapFile;
    public static String mapFileFormat;
    public static String shortestPathAlgorithm;
    public static double nodeMappingTolerance;
    public static String trajStartEndSelectionApproach;
    public static String interWorkersDataSharing;
    public static Boolean sync;
    public static Boolean randomizeTimeInBatch;
    public static double syncPercentage;
    public static double displacementMetersPerSecond;
    public static String trajectoryType;
    public static List<List<Double>> trajStartEndCoordinatePairs;

    public static List<List<Double>> trajStartPolygons;

    public static List<List<Double>> trajEndPolygons;


    /* redis */
    public static String redisAddresses;
    public static String   redisServerType;

    public Params() throws NullPointerException, IllegalArgumentException, NumberFormatException, FactoryException {
        ConfigType config = getYamlConfig(YAML_PATH);

        /* Cluster */
        clusterMode = config.isClusterMode();

        /* Parallelism */
        parallelism = config.getParallelism();

        /* output */
        try {
            if ((outputOption = (String)config.getOutput().get("option")) == null) {
                throw new NullPointerException("outputOption is " + config.getOutput().get("option"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("outputOption : " + e);
        }

        try {
            if ((outputFormat = (String)config.getOutput().get("outputFormat")) == null) {
                throw new NullPointerException("outputFormat is " + config.getOutput().get("outputFormat"));
            }
            else {
                List<String> validParam = Arrays.asList("GeoJSON", "WKT");
                if (!validParam.contains(outputFormat)) {
                    throw new IllegalArgumentException(
                            "outputFormat is " + outputFormat + ". " +
                                    "Valid value is \"GeoJSON\" or \"WKT\".");
                }
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("outputFormat : " + e);
        }

        /* output - kafka */
        try {
            Map<String, Object> kafka;
            if ((kafka = (Map<String, Object>)config.getOutput().get("kafka")) == null) {
                throw new NullPointerException("kafka is " + config.getOutput().get("kafka"));
            }

            try {
                if ((outputTopicName = (String)kafka.get("outputTopicName")) == null) {
                    throw new NullPointerException("outputOption is " + config.getOutput().get("outputOption"));
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("outputTopicName : " + e);
            }
            try {
                if ((bootStrapServers = (String)kafka.get("bootStrapServers")) == null) {
                    throw new NullPointerException("bootStrapServers is " + kafka.get("bootStrapServers"));
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("bootStrapServers : " + e);
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("kafka : " + e);
        }

        /* output - file */
        try {
            Map<String, Object> file;
            if ((file = (Map<String, Object>)config.getOutput().get("file")) == null) {
                throw new NullPointerException("file is " + config.getOutput().get("file"));
            }
            if ((outputDirName = (String)file.get("outputDirName")) == null) {
                outputDirName = DEFAULT_OUTPUT_DIR_NAME;
            }
            if(file.get("outputRollingRolloverInterval") != null) {
                outputRollingRolloverInterval = (long)file.get("outputRollingRolloverInterval");
            }
            if(file.get("outputRollingInactivityInterval") != null) {
                outputRollingInactivityInterval = (long)file.get("outputRollingInactivityInterval");
            }
            if(file.get("outputRollingMaxPartSize") != null) {
                outputRollingMaxPartSize = (long)file.get("outputRollingMaxPartSize");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("file option : " + e);
        }

        /* output - data */
        try {
            Map<String, Object> data;
            if ((data = (Map<String, Object>)config.getOutput().get("data")) == null) {
                throw new NullPointerException("file is " + config.getOutput().get("file"));
            }
            try {
                if ((dateFormat = (String)data.get("dateFormat")) == null) {
                    throw new NullPointerException("dateFormat is " + data.get("dateFormat"));
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("dateFormat : " + e);
            }

            try {
                if ((initialTimeStamp = (String)data.get("initialTimeStamp")) == null) {
                    throw new NullPointerException("initialTimeStamp is " + data.get("initialTimeStamp"));
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("initialTimeStamp : " + e);
            }

            try {
                if (data.get("timeStep") == null) {
                    throw new NullPointerException("timeStep is " + data.get("timeStep"));
                } else {
                    //nRows = (int)config.getData().get("nRows");
                    timeStep = ((Number) data.get("timeStep")).intValue();
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("timeStep : " + e);
            }

            try {
                if(data.get("randomizeTimeInBatch") == null) {
                    throw new NullPointerException("randomizeTimeInBatch is " + data.get("randomizeTimeInBatch"));
                }
                else {
                    randomizeTimeInBatch = (Boolean)data.get("randomizeTimeInBatch");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("randomizeTimeInBatch : " + e);
            }

            try {
                if ((objIDRange = (ArrayList)data.get("objIDRange")) == null) {
                    throw new NullPointerException("objIDRange is " + data.get("objIDRange"));
                }
                if (objIDRange.size() != 2) {
                    throw new IllegalArgumentException("objIDRange num is " + objIDRange.size());
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("objIDRange : " + e);
            }

            try {
                if(data.get("nRows") == null) {
                    throw new NullPointerException("nRows is " + data.get("nRows"));
                }
                else {
                    //nRows = (int)config.getData().get("nRows");
                    nRows = ((Number)data.get("nRows")).longValue();
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("nRows : " + e);
            }
            try {
                if(data.get("consecutiveTrajTuplesIntervalMilliSec") == null) {
                    throw new NullPointerException("consecutiveTrajTuplesIntervalMilliSec is " + data.get("consecutiveTrajTuplesIntervalMilliSec"));
                }
                else {
                    consecutiveTrajTuplesIntervalMilliSec = (int)data.get("consecutiveTrajTuplesIntervalMilliSec");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("consecutiveTrajTuplesIntervalMilliSec: " + e);
            }

        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("data : " + e);
        }

        /* query */
        try {
            String coordinateReferenceSystemStr;
            if ((coordinateReferenceSystemStr = (String)config.getQuery().get("coordinateReferenceSystem")) == null) {
                throw new NullPointerException("coordinate reference system is " + config.getQuery().get("coordinateReferenceSystem"));
            }

            if(coordinateReferenceSystemStr.equalsIgnoreCase("WGS84") || coordinateReferenceSystemStr.equalsIgnoreCase("EPSG:4326")){
                this.coordinateReferenceSystem = CRS.decode("EPSG:4326", true);

            }else if(coordinateReferenceSystemStr.equalsIgnoreCase("Google") || coordinateReferenceSystemStr.equalsIgnoreCase("EPSG:3857")){
                coordinateReferenceSystem = CRS.decode("EPSG:3857", true);
            }
            else{  // Default
                coordinateReferenceSystem = DefaultGeographicCRS.WGS84;
            }

        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("coordinateReferenceSystem option : " + e);
        }

        /* mappedTrajectories */
        try {
            Map<String, Object> mappedTrajectories;

            if ((mappedTrajectories = (Map<String, Object>)config.getQuery().get("mappedTrajectories")) == null) {
                throw new NullPointerException("file is " + config.getOutput().get("file"));
            }

            try {
                if(mappedTrajectories.get("mapFile") == null) {
                    throw new NullPointerException("mapFile is " + mappedTrajectories.get("mapFile"));
                }
                else {
                    mapFile = (String)mappedTrajectories.get("mapFile");
//                    String mapfilePath = new File(".").getAbsoluteFile().getParent() + File.separator +
//                            "conf" + File.separator + (String)mappedTrajectories.get("mapFile");
//                    mapFile = mapfilePath;
                }

            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("mapFile : " + e);
            }

            try {
                if(mappedTrajectories.get("format") == null) {
                    throw new NullPointerException("mapFile format is " + mappedTrajectories.get("format"));
                }
                else {
                    mapFileFormat = (String)mappedTrajectories.get("format");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("format : " + e);
            }

            try {
                if(mappedTrajectories.get("interWorkersDataSharing") == null) {
                    throw new NullPointerException("interWorkersDataSharing is " + mappedTrajectories.get("interWorkersDataSharing"));
                }
                else {
                    interWorkersDataSharing = (String)mappedTrajectories.get("interWorkersDataSharing");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("interWorkersDataSharing : " + e);
            }

            try {
                if(mappedTrajectories.get("sync") == null) {
                    throw new NullPointerException("sync is " + mappedTrajectories.get("sync"));
                }
                else {
                    sync = (Boolean)mappedTrajectories.get("sync");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("sync : " + e);
            }


            try {
                if(mappedTrajectories.get("syncPercentage") == null) {
                    throw new NullPointerException("syncPercentage is " + mappedTrajectories.get("syncPercentage"));
                }
                else {
                    syncPercentage = (double)mappedTrajectories.get("syncPercentage");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("syncPercentage : " + e);
            }

            try {
                if(mappedTrajectories.get("shortestPathAlgorithm") == null) {
                    throw new NullPointerException("Shortest Path Algorithm is " + mappedTrajectories.get("shortestPathAlgorithm"));
                }
                else {
                    shortestPathAlgorithm = (String)mappedTrajectories.get("shortestPathAlgorithm");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("shortestPathAlgorithm : " + e);
            }

            try {
                if(mappedTrajectories.get("nodeMappingTolerance") == null) {
                    throw new NullPointerException("nodeMappingTolerance is " + mappedTrajectories.get("nodeMappingTolerance"));
                }
                else {
                    nodeMappingTolerance = (double)mappedTrajectories.get("nodeMappingTolerance");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("nodeMappingTolerance : " + e);
            }

            try {
                if ((trajectoryType = (String)mappedTrajectories.get("trajectoryType")) == null) {
                    throw new NullPointerException("trajectoryType is " + config.getQuery().get("trajectoryType"));
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("trajectoryType: " + e);
            }

            try {
                if(mappedTrajectories.get("trajStartEndSelectionApproach") == null) {
                    throw new NullPointerException("Trajectory Start and End Selection Approach " + mappedTrajectories.get("trajStartEndSelectionApproach"));
                }
                else {
                    trajStartEndSelectionApproach = (String)mappedTrajectories.get("trajStartEndSelectionApproach");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("trajStartEndSelectionApproach : " + e);
            }

            try {
                if(mappedTrajectories.get("displacementMetersPerSecond") == null) {
                    throw new NullPointerException("displacementMetersPerSecond is " + mappedTrajectories.get("displacementMetersPerSecond"));
                }
                else {
                    displacementMetersPerSecond = (double)mappedTrajectories.get("displacementMetersPerSecond");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("displacementMetersPerSecond : " + e);
            }

            try {
                if(mappedTrajectories.get("trajStartEndCoordinatePairs") == null) {
                    throw new NullPointerException("Trajectory Start-End Coordinates are " + mappedTrajectories.get("trajStartEndCoordinatePairs"));
                }
                else {
                    trajStartEndCoordinatePairs = (List<List<Double>>)mappedTrajectories.get("trajStartEndCoordinatePairs");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("trajStartEndCoordinatePairs : " + e);
            }

            try {
                if(mappedTrajectories.get("trajStartPolygons") == null) {
                    throw new NullPointerException("Trajectory Start Polygons are " + mappedTrajectories.get("trajStartPolygons"));
                }
                else {
                    trajStartPolygons = (List<List<Double>>)mappedTrajectories.get("trajStartPolygons");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("trajStartPolygons : " + e);
            }

            try {
                if(mappedTrajectories.get("trajEndPolygons") == null) {
                    throw new NullPointerException("Trajectory End Polygons are " + mappedTrajectories.get("trajEndPolygons"));
                }
                else {
                    trajEndPolygons = (List<List<Double>>)mappedTrajectories.get("trajEndPolygons");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("trajEndPolygons : " + e);
            }


            try {
                if(mappedTrajectories.get("displacementMetersPerSecond") == null) {
                    throw new NullPointerException("displacementMetersPerSecond is " + mappedTrajectories.get("displacementMetersPerSecond"));
                }
                else {
                    displacementMetersPerSecond = (double)mappedTrajectories.get("displacementMetersPerSecond");
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("displacementMetersPerSecond : " + e);
            }


        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("mappedTrajectories : " + e);
        }

        /* redis */
        try {

//            try {
//                if ((redisHost = (String)config.getRedis().get("redisHost")) == null) {
//                    throw new NullPointerException("redisHost is " + config.getRedis().get("redisHost"));
//                }
//            }
//            catch (ClassCastException e) {
//                throw new IllegalArgumentException("redisHost : " + e);
//            }

            try {
                if ((redisAddresses = (String)config.getRedis().get("redisAddresses")) == null) {
                    throw new NullPointerException("redisAddresses is " + config.getRedis().get("redisAddresses"));
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("redisAddresses : " + e);
            }

            try {
                if ((redisServerType = (String)config.getRedis().get("redisServerType")) == null) {
                    throw new NullPointerException("redisServerType is " + config.getRedis().get("redisServerType"));
                }
            }
            catch (ClassCastException e) {
                throw new IllegalArgumentException("redisServerType : " + e);
            }

        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("redis : " + e);
        }
    }

    private ConfigType getYamlConfig(String path) {
        File file = new File(path);
        Constructor constructor = new Constructor(ConfigType.class);
        Yaml yaml = new Yaml(constructor);
        FileInputStream input;
        InputStreamReader stream;
        try {
            input = new FileInputStream(file);
            stream = new InputStreamReader(input, "UTF-8");
            return (ConfigType) yaml.load(stream);
        }
        catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
        catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String toString() {
        return  "clusterMode = " + clusterMode + ", " +
                "\n" +
                "parallelism = " + parallelism + ", " +
                "\n" +
                "outputOption = " + outputOption + ", " +
                "format = " + outputFormat + ", " +
                "\n" +
                "outputTopicName = " + outputTopicName + ", " +
                "bootStrapServers = " + bootStrapServers + ", " +
                "\n" +
                "outputDirName = " + outputDirName + ", " +
                "\n" +
                "dateFormat = " + dateFormat + ", " +
                "objIDRange = " + objIDRange + ", " +
                "nRows = " + nRows + ", " +
                "\nconsecutiveTrajTuplesIntervalMilliSec = " + consecutiveTrajTuplesIntervalMilliSec + ", " +
                "\n" +
                "trajectoryType = " + trajectoryType + ", " +
                "\n" +
                "mapFile = " + mapFile + ", " +
                "mapFileFormat = " + mapFileFormat +
                "\n" +
                "shortestPathAlgorithm = " + shortestPathAlgorithm + ", " +
                "nodeMappingTolerance = " + nodeMappingTolerance + ", " +
                "trajectoryType = " + trajectoryType + ", " +
                "\n" +
                "interWorkersDataSharing = " +  interWorkersDataSharing + ", " +
                "\n" + "sync = " + sync + ", " +
                "syncPercentage = " + syncPercentage + ", " +
                "\n" +
                "trajStartEndSelectionApproach = " + trajStartEndSelectionApproach + ", " +
                "\n" +
                "trajStartEndCoordinatePairs = " + trajStartEndCoordinatePairs + ", " +
                "\n" +
                "trajStartPolygons = " + trajStartPolygons + ", " +
                "\n" +
                "trajEndPolygons = " + trajEndPolygons + ", " +
                "\n" +
                "displacementMetersPerSecond = " + displacementMetersPerSecond;
    }
}