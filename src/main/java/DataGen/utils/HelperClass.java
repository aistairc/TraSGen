package DataGen.utils;

//import jdk.internal.util.xml.impl.Pair;
import DataGen.geometryGenerator.GeometryGenerator;
import DataGen.geometryGenerator.LineStringGenerator;
import DataGen.geometryGenerator.MultiGenerator;
import DataGen.geometryGenerator.PolygonGenerator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.akka.org.jboss.netty.util.internal.ThreadLocalRandom;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.*;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class HelperClass {

    private static Random generator = new Random(12345);

    // Methods
    //Generate Random XY coordinate
    public static Coordinate getRandomXYTuple(Envelope env){

        double x = getRandomDoubleInRange(env.getMinX(), env.getMaxX());
        double y = getRandomDoubleInRange(env.getMinY(), env.getMaxY());

        return new Coordinate(x,y);
    }

    //Generate Random XY coordinate based on past coordinate
    public static Coordinate getGaussianDistributedXYTuple(Coordinate pastCoordinate, Envelope env, double variance){

        Random fRandom = new Random();
        double x;
        double y;

        // Generating x and y gaussian random variable within given variance range
        do {
            x = pastCoordinate.x + fRandom.nextGaussian() * variance;
        } while(!withinRange(x, env.getMinX(), env.getMaxX()));

        do {
            y = pastCoordinate.y + fRandom.nextGaussian() * variance;
        } while(!withinRange(y, env.getMinY(), env.getMaxY()));

        return new Coordinate(x, y);
    }

    public static Envelope getRandomBBox(Envelope bBox){
        double x1 = getRandomDoubleInRange(bBox.getMinX(), bBox.getMaxX());
        double y1 = getRandomDoubleInRange(bBox.getMinY(), bBox.getMaxY());
        double x2 = getRandomDoubleInRange(bBox.getMinX(), bBox.getMaxX());
        double y2 = getRandomDoubleInRange(bBox.getMinY(), bBox.getMaxY());

        return new Envelope(Math.min(x1, x2), Math.max(x1, x2), Math.min(y1, y2), Math.max(y1, y2));
    }

    public static Envelope getGaussianDistributedRandomBBox(Envelope lastBBox, Envelope env, double seriesVar){
        Coordinate c1 = getGaussianDistributedXYTuple(new Coordinate(lastBBox.getMinX(), lastBBox.getMinY()), env, seriesVar);
        Coordinate c2 = getGaussianDistributedXYTuple(new Coordinate(lastBBox.getMaxX(), lastBBox.getMaxY()), env, seriesVar);

        double x1 = c1.getX();
        double y1 = c1.getY();
        double x2 = c2.getX();
        double y2 = c2.getY();

        return new Envelope(Math.min(x1, x2), Math.max(x1, x2), Math.min(y1, y2), Math.max(y1, y2));
    }

    public static double getRandomDoubleInRange(double minRange, double maxRange){
        if(maxRange - minRange <= 0){
            return minRange;
        }
        else {
            return minRange + ThreadLocalRandom.current().nextDouble() * (maxRange - minRange);
        }
        //return minRange + Math.random() * (maxRange - minRange);
    }

    public static int getRandomIntInRange(int minRange, int maxRange){
        if(maxRange - minRange <= 0){
            return minRange;
        }
        else {
            return minRange + ThreadLocalRandom.current().nextInt(maxRange - minRange);
        }
        //return minRange + (int)Math.random() * (maxRange - minRange);
    }

    public static int getRandomIntInRangeWithoutThread(int minRange, int maxRange){
        if(maxRange - minRange <= 0){
            return minRange;
        }
        else {
            return minRange + generator.nextInt(maxRange - minRange);
        }

    }


    public static boolean withinRange(double val, double minRange, double maxRange){
        return (val >= minRange && val <= maxRange);
    }

    public static Geometry generatePoint(Coordinate coordinate){

        GeometryFactory geometryFactory = new GeometryFactory();
        return geometryFactory.createPoint(coordinate);
    }

    public static Geometry generatePolygon(int nPoints, int nHoles, Envelope bBox, int geometryGenAlgorithm){

        GeometryFactory geometryFactory = new GeometryFactory();
        PolygonGenerator polygonGenerator = GeometryGenerator.createPolygonGenerator();
        polygonGenerator.setGeometryFactory(geometryFactory);
        polygonGenerator.setBoundingBox(bBox);
        polygonGenerator.setNumberPoints(nPoints);
        polygonGenerator.setNumberHoles(nHoles);
        polygonGenerator.setGenerationAlgorithm(geometryGenAlgorithm); // 0: Box, 1: Arc, 2: Rnd
        return polygonGenerator.create();
    }

    public static Geometry generateLineString(int nPoints, Envelope bBox, int geometryGenAlgorithm){

        GeometryFactory geometryFactory = new GeometryFactory();
        LineStringGenerator lineStringGenerator = GeometryGenerator.createLineStringGenerator();
        lineStringGenerator.setGeometryFactory(geometryFactory);
        lineStringGenerator.setBoundingBox(bBox);
        lineStringGenerator.setNumberPoints(nPoints);
        lineStringGenerator.setGenerationAlgorithm(geometryGenAlgorithm); //0: Vertical, 1: Horizontal, 2: Arc, 3:Random
        return lineStringGenerator.create();
    }

    public static Geometry generateMultiPoint(int numGeometries, Envelope bBox, int multiGeometryGenAlgorithm){

        GeometryFactory geometryFactory = new GeometryFactory();
        MultiGenerator multiPointGenerator = MultiGenerator.createMultiPointGenerator();
        multiPointGenerator.setNumberGeometries(numGeometries);
        multiPointGenerator.setGeometryFactory(geometryFactory);
        multiPointGenerator.setBoundingBox(bBox);
        multiPointGenerator.setGenerationAlgorithm(multiGeometryGenAlgorithm); //0: Box, 1: Vertical, 2: Horizontal
        return multiPointGenerator.create();
    }

    public static Geometry generateMultiLineString(int nPoints, int nGeometries, Envelope bBox, int multiGeometryGenAlgorithm, int geometryGenAlgorithm){

        GeometryFactory geometryFactory = new GeometryFactory();
        MultiGenerator multiLineStringGenerator = MultiGenerator.createMultiLineStringGenerator(nPoints, geometryGenAlgorithm);
        multiLineStringGenerator.setNumberGeometries(nGeometries);
        multiLineStringGenerator.setGeometryFactory(geometryFactory);
        multiLineStringGenerator.setBoundingBox(bBox);
        multiLineStringGenerator.setGenerationAlgorithm(multiGeometryGenAlgorithm); //0: Box, 1: Vertical, 2: Horizontal, 3:Random
        return multiLineStringGenerator.create();

        //throw new NullPointerException("Could not create linestring");
    }

    public static Geometry generateMultiPolygon(int nPoints, int nHoles, int nGeometries, Envelope bBox, int multiGeometryGenAlgorithm, int geometryGenAlgorithm){

        GeometryFactory geometryFactory = new GeometryFactory();
        MultiGenerator multiPolygonGenerator = MultiGenerator.createMultiPolygonGenerator(nPoints, nHoles, geometryGenAlgorithm);
        multiPolygonGenerator.setNumberGeometries(nGeometries);
        multiPolygonGenerator.setGeometryFactory(geometryFactory);
        multiPolygonGenerator.setBoundingBox(bBox);
        multiPolygonGenerator.setGenerationAlgorithm(multiGeometryGenAlgorithm); //0: Box, 1: Vertical, 2: Horizontal
        return multiPolygonGenerator.create();
    }


    // Classes

    public static class objIDKeySelector implements KeySelector<Integer,Integer> {
        @Override
        public Integer getKey(Integer objID) throws Exception {
            return objID;
        }
    }

    public static class objIDKeySelectorWithBatchID implements KeySelector<Tuple2<Integer,Long>, Integer> {
        @Override
        public Integer getKey(Tuple2<Integer,Long> objID) throws Exception {
            return objID.f0;
        }
    }



    /*
        Java Faker
        Tutorial: https://www.baeldung.com/java-faker
		Faker faker = new Faker(new Locale("ja"), new ThreadLocalRandom());
		Faker faker2 = new Faker(Locale.JAPAN);

		System.out.println("faker : " + faker.random().nextLong());
		System.out.println("faker : " + faker.address().city());
		System.out.println("faker : " + faker.address().cityName());

		System.out.println("faker : " + faker2.address().latitude());
		System.out.println("faker : " + faker.address().longitude());
		 */

    /*
    // Generates completely random linestring within given boundary
    public static List<Coordinate> generateRandomLineString(int numLineSegments, Envelope env){

        if(numLineSegments <= 0)
            return null;

        List<Coordinate> lineStringCoordinates = new ArrayList<>();
        for (int i = 0; i <= numLineSegments; i++) {
            lineStringCoordinates.add(getRandomXYTuple(env));
        }

        return lineStringCoordinates;
    }

    // Generates random linestring with linesegments' length bounded by min and max variance
    public static List<Coordinate> generateRandomLineString(int numLineSegments, Envelope env, double lineSegmentLengthVar){

        if(numLineSegments <= 0)
            return null;

        List<Coordinate> lineStringCoordinates = new ArrayList<>();
        // adding first random coordinate
        lineStringCoordinates.add(getRandomXYTuple(env));

        // next coordinates are based on min and max line segment length variance
        for (int i = 1; i <= numLineSegments; i++) {
            Coordinate pastCoord = lineStringCoordinates.get(i-1);
            lineStringCoordinates.add(getGaussianDistributedXYTuple(pastCoord, env, lineSegmentLengthVar));
        }

        return lineStringCoordinates;
    }

    // Generates random linestring with linesegments' length bounded by min and max variance and based on past lineString centroid coordinate
    public static List<Coordinate> generateRandomGaussianLineString(Coordinate pastCentroid, int numLineSegments, Envelope env, double lineSegmentLengthVar, double seriesVar){

        if(numLineSegments <= 0)
            return null;

        List<Coordinate> lineStringCoordinates = new ArrayList<>();
        // adding first random Gaussian coordinate based on pastCentroid and min and max Series Variance
        lineStringCoordinates.add(getGaussianDistributedXYTuple(pastCentroid, env, seriesVar));

        // next coordinates are based on min and max line segment length variance and pastCoordinate
        for (int i = 1; i <= numLineSegments; i++) {
            Coordinate pastCoord = lineStringCoordinates.get(i-1);
            lineStringCoordinates.add(getGaussianDistributedXYTuple(pastCoord, env, lineSegmentLengthVar));
        }

        return lineStringCoordinates;
    }

    // Generates completely random polygon within given boundary
    public static List<Coordinate> generateRandomPolygon(int numSides, Envelope env){

        if(numSides <= 0)
            return null;

        List<Coordinate> polyCoordinates = generateRandomLineString(numSides, env);
        assert polyCoordinates != null;
        polyCoordinates.remove(numSides);
        polyCoordinates.add(polyCoordinates.get(0));

        return polyCoordinates;
    }

    // Generates random polygon with sides' length bounded by min and max variance
    public static List<Coordinate> generateRandomPolygon(int numSides, Envelope env, double sideLengthVar){

        if(numSides <= 0)
            return null;

        List<Coordinate> polyCoordinates = generateRandomLineString(numSides, env, sideLengthVar);
        assert polyCoordinates != null;
        polyCoordinates.remove(numSides);
        polyCoordinates.add(polyCoordinates.get(0));

        return polyCoordinates;
    }

    // Generates random polygon with sides' length bounded by min and max variance and based on past polygon centroid coordinate
    public static List<Coordinate> generateRandomGaussianPolygon(Coordinate pastCentroid, int numSides, Envelope env, double sideLengthVar, double seriesVar){

        if(numSides <= 0)
            return null;

        List<Coordinate> polyCoordinates = generateRandomGaussianLineString(pastCentroid, numSides, env, sideLengthVar, seriesVar);
        assert polyCoordinates != null;
        polyCoordinates.remove(numSides);
        polyCoordinates.add(polyCoordinates.get(0));

        return polyCoordinates;
    }
     */

    public static Date localDateTimeToDate(LocalDateTime localDateTime) {
        ZoneId zone = ZoneId.systemDefault();
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zone);
        Instant instant = zonedDateTime.toInstant();
        return Date.from(instant);
    }

    public static String TimeStamp( String dateFormat, String initialTimeStamp, int timeStepinMilliSec, Long batchID, Random timeGen, boolean randomizeTimeInBatch) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        LocalDateTime ldt = LocalDateTime.parse(initialTimeStamp, formatter);
        ldt = ldt.plus(timeStepinMilliSec * (batchID-1L), ChronoUnit.MILLIS);

        if (randomizeTimeInBatch) {
            int randomVariation =  timeGen.nextInt(timeStepinMilliSec + 1);
            ldt = ldt.plus(randomVariation, ChronoUnit.MILLIS);
        }

        return DateTimeFormatter.ofPattern(dateFormat).format(ldt);
    }


    public static Coordinate[] sortPolygonCoordinate(Coordinate[] coordinates) {
        Coordinate[] ret = new Coordinate[coordinates.length];
        Coordinate[] source = getArrayWithoutN(coordinates, coordinates[0]);
        Coordinate target = coordinates[0];
        int i = 0;
        ret[i++] = target;
        while (source.length > 0) {
            Coordinate c = getNearestCoordinate(target, source);
            ret[i++] = c;
            source = getArrayWithoutN(source, c);
            target = c;
        }
        return ret;
    }

    public static Coordinate[] sortPolygonCoordinateByAngle(Coordinate[] coordinates) {
        // get center position coordinate
        GeometryFactory gf = new GeometryFactory();
        Point[] points = new Point[coordinates.length];
        for (int i = 0; i < points.length; i++) {
            points[i] = gf.createPoint(coordinates[i]);
        }
        MultiPoint multiPoint = gf.createMultiPoint(points);
        Coordinate center = multiPoint.getCentroid().getCoordinate();
        // sort by radian
        return sortCoordinate(center, createCoordinateRadian(center, coordinates));
    }

    private static CoordinateRadian[] createCoordinateRadian(Coordinate center, Coordinate[] coordinates) {
        CoordinateRadian[] coordinateRadians = new CoordinateRadian[coordinates.length];
        Coordinate base = new Coordinate(center.getX(), (center.getY() + 1));
        int i = 0;
        for (Coordinate c: coordinates) {
            coordinateRadians[i] = new CoordinateRadian(c, getRadian(center, base, c), i);
            i++;
        }
        return coordinateRadians;
    }

    private static Coordinate[] sortCoordinate(Coordinate center, CoordinateRadian[] coordinateRadians) {
        double[] radians = new double[coordinateRadians.length];
        Coordinate[] coordinates = new Coordinate[coordinateRadians.length];
        int i = 0;
        for (CoordinateRadian coordinateRadian: coordinateRadians) {
            radians[i++] = coordinateRadian.getRadian();
        }
        // sort by radian
        Arrays.sort(radians);
        CoordinateRadian[] sortedCoordinateRadians = new CoordinateRadian[coordinates.length];
        i = 0;
        for (double radian: radians) {
            for (CoordinateRadian coordinateRadian: coordinateRadians) {
                if (coordinateRadian.getRadian() == radian) {
                    if (i == 0) {
                        sortedCoordinateRadians[i++] = coordinateRadian;
                        break;
                    }
                    else {
                        if (!isIncludeCountFromCoordinateRadians(sortedCoordinateRadians, coordinateRadian.getCount())) {
                            sortedCoordinateRadians[i++] = coordinateRadian;
                            break;
                        }
                    }
                }
            }
        }
        // sort by distancedistance if there are things of same radian
        CoordinateRadian[] sortedCoordinateRadiansByDistance = sortCoordinateRadianByDistance(center, sortedCoordinateRadians);
        return convertCoordinateRadiansToCoordinates(sortedCoordinateRadiansByDistance);
    }

    private static boolean isIncludeCountFromCoordinateRadians(CoordinateRadian[] coordinateRadians, int count) {
        for (CoordinateRadian coordinateRadian: coordinateRadians) {
            if (coordinateRadian == null) {
                break;
            }
            if (coordinateRadian.getCount() == count) {
                return true;
            }
        }
        return false;
    }

    private static Coordinate[] getArrayWithoutN(Coordinate[] coordinates, Coordinate n) {
        ArrayList<Coordinate> list = new ArrayList<>();
        int i = 0;
        for (Coordinate c: coordinates) {
            if (c != null && !c.equals(n)) {
                list.add(c);
            }
        }
        Coordinate[] ret = list.toArray(new Coordinate[list.size()]);
        return ret;
    }

    private static Coordinate getNearestCoordinate(Coordinate target, Coordinate[] coordinates) {
        double minDis = Double.MAX_VALUE;
        int index = 0;
        for (int i = 0; i < coordinates.length; i++) {
            Coordinate c = coordinates[i];
            double dis = target.distance(c);
            if (minDis > dis) {
                minDis = dis;
                index = i;
            }
        }
        return coordinates[index];
    }

    private static double getRadian(Coordinate center, Coordinate p1, Coordinate p2) {
        double result = Math.atan2(p2.y - center.y, p2.x - center.x) -
                Math.atan2(p1.y - center.y, p1.x - center.x);
        return result;
    }

    private static CoordinateRadian[] sortCoordinateRadianByDistance(Coordinate center, CoordinateRadian[] coordinateRadians) {
        List<CoordinateRadian> sortedList = new ArrayList<>();
        List<CoordinateRadian> list = new ArrayList<>();
        for (int i = 1; i < coordinateRadians.length; i++) {
            // same radian
            if (coordinateRadians[i-1].getRadian() == coordinateRadians[i].getRadian()) {
                if (list.size() == 0) {
                    sortedList.remove(coordinateRadians[i-1]);
                    list.add(coordinateRadians[i-1]);
                }
                list.add(coordinateRadians[i]);
            }
            // different radian
            else {
                if (i == 1) {
                    sortedList.add(coordinateRadians[i-1]);
                }
                // there are things of same radian
                if (list.size() > 0) {
                    addSortedList(center, sortedList, list, coordinateRadians);
                    sortedList.add(coordinateRadians[i]);
                }
                // there aren't things of same radian
                else {
                    sortedList.add(coordinateRadians[i]);
                }
            }
        }
        // there are things of same radian
        if (list.size() > 0) {
            addSortedList(center, sortedList, list, coordinateRadians);
        }
        return sortedList.toArray(new CoordinateRadian[0]);
    }

    private static List<CoordinateRadian> addSortedList(Coordinate center, List<CoordinateRadian> sortedList,
                                                        List<CoordinateRadian> sameRadianList, CoordinateRadian[] coordinateRadians) {
        Coordinate target;
        if (sortedList.size() == 0) {
            target = center;
        }
        else {
            target = sortedList.get(sortedList.size()-1).getCoordinate();
        }
        Coordinate[] source = new Coordinate[sameRadianList.size()];
        for (int j = 0; j < sameRadianList.size(); j++) {
            source[j] = sameRadianList.get(j).getCoordinate();
        }
        Coordinate[] sortedCoordinate = new Coordinate[sameRadianList.size()];
        sameRadianList.clear();
        int k = 0;
        // sort by distance of coordinates
        while (source.length > 0) {
            Coordinate c = getNearestCoordinate(target, source);
            sortedCoordinate[k++] = c;
            source = getArrayWithoutN(source, c);
        }
        List<CoordinateRadian> tmpSortedList = sortedCoordinate(sortedCoordinate, coordinateRadians);
        for (CoordinateRadian coordinateRadian : tmpSortedList) {
            sortedList.add(coordinateRadian);
        }
        return sortedList;
    }

    private static List<CoordinateRadian> sortedCoordinate(
                Coordinate[] coordinates, CoordinateRadian[] coordinateRadians) {
        List<CoordinateRadian> ret = new ArrayList<>();
        for (Coordinate c: coordinates) {
            for (CoordinateRadian coordinateRadian: coordinateRadians) {
                if (c.equals(coordinateRadian.getCoordinate())) {
                    ret.add(coordinateRadian);
                    break;
                }
            }
        }
        return ret;
    }

    private static Coordinate[] convertCoordinateRadiansToCoordinates(CoordinateRadian[] coordinateRadians) {
        Coordinate[] ret = new Coordinate[coordinateRadians.length];
        int i = 0;
        for (CoordinateRadian coordinateRadian: coordinateRadians) {
            ret[i++] = coordinateRadian.getCoordinate();
        }
        return ret;
    }

    public static double getDisplacementMetersPerSecond(int roadCapacity, Coordinate edgeSourceCoordinates,
                                                        Coordinate edgeTargetCoordinates, int currentRoadTraffic,
                                                        double displacementMetersPerSecond, CoordinateReferenceSystem crs,
                                                        GeodeticCalculator gc){

        double roadLength = SpatialFunctions.getDistanceInMeters(edgeSourceCoordinates, edgeTargetCoordinates, crs, gc);
        double carLength = 10; //meters
        double lanes = 2;

        if (roadLength <= carLength)
            roadCapacity = 1;
        else
            roadCapacity = (int) ((roadLength/carLength) * lanes);

        if(currentRoadTraffic <= roadCapacity){
            return displacementMetersPerSecond;
        }

        return ((float)roadCapacity/currentRoadTraffic)*displacementMetersPerSecond;
    }
}
