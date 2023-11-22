package DataGen.timeSeriesGenerators;

import DataGen.inputParameters.Params;
import DataGen.timeSeriesGenerators.random.HotspotDistribution;
import DataGen.timeSeriesGenerators.random.HotspotGaussianRandomWalk;
import DataGen.timeSeriesGenerators.random.RandomDistribution;
import DataGen.timeSeriesGenerators.random.RandomWalk;
import DataGen.utils.HelperClass;
import DataGen.utils.Serialization;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

public class MultiPolygonStreamGenerator implements StreamGenerator {
    private final int minPoints;
    private final int maxPoints;
    private final int minPolygonHoles;
    private final int maxPolygonHoles;
    private final int minGeometries;
    private final int maxGeometries;
    private RandomDistribution randomDistribution;
    private final double seriesVariance;
    private int multiGeometryGenAlgorithm = 0;
    private int geometryGenAlgorithm = 0;
    private String format = "GeoJSON";
    private String initialTimeStamp = null;
    private String dateFormat = null;
    private int timeStepinMilliSec = 0;
    private Envelope seriesBBox;
    private Random timeGen = null;
    boolean randomizeTimeInBatch;

    public MultiPolygonStreamGenerator() {

        this.minPoints = NUM_OF_POINTS;
        this.maxPoints = NUM_OF_POINTS;
        this.minPolygonHoles = NUM_OF_POLYGON_HOLES;
        this.maxPolygonHoles = NUM_OF_POLYGON_HOLES;
        this.minGeometries = NUM_OF_GEOMETRIES;
        this.maxGeometries = NUM_OF_GEOMETRIES;
        this.seriesVariance = SERIES_VARIANCE;
    }

    public MultiPolygonStreamGenerator(int minPoints, int maxPoints) {

        this.minPoints = minPoints;
        this.maxPoints = maxPoints;
        this.minPolygonHoles = NUM_OF_POLYGON_HOLES;
        this.maxPolygonHoles = NUM_OF_POLYGON_HOLES;
        this.minGeometries = NUM_OF_GEOMETRIES;
        this.maxGeometries = NUM_OF_GEOMETRIES;
        this.seriesVariance = SERIES_VARIANCE;
    }

    public MultiPolygonStreamGenerator(int minPoints, int maxPoints, int minHoles, int maxHoles) {

        this.minPoints = minPoints;
        this.maxPoints = maxPoints;
        this.minPolygonHoles = minHoles;
        this.maxPolygonHoles = maxHoles;
        this.minGeometries = NUM_OF_GEOMETRIES;
        this.maxGeometries = NUM_OF_GEOMETRIES;
        this.seriesVariance = SERIES_VARIANCE;
    }

    public MultiPolygonStreamGenerator(int minPoints, int maxPoints, int minHoles, int maxHoles, int minGeometries, int maxGeometries) {

        this.minPoints = minPoints;
        this.maxPoints = maxPoints;
        this.minPolygonHoles = minHoles;
        this.maxPolygonHoles = maxHoles;
        this.minGeometries = minGeometries;
        this.maxGeometries = maxGeometries;
        this.seriesVariance = SERIES_VARIANCE;
    }

    public MultiPolygonStreamGenerator(int minPoints, int maxPoints, int minHoles, int maxHoles, double seriesVariance) {

        this.minPoints = minPoints;
        this.maxPoints = maxPoints;
        this.minPolygonHoles = minHoles;
        this.maxPolygonHoles = maxHoles;
        this.minGeometries = NUM_OF_GEOMETRIES;
        this.maxGeometries = NUM_OF_GEOMETRIES;
        this.seriesVariance = seriesVariance;
    }

    public MultiPolygonStreamGenerator(int minPoints, int maxPoints, int minHoles, int maxHoles,
                                       int minGeometries, int maxGeometries, double seriesVariance, int multiGeometryGenAlgorithm, int geometryGenAlgorithm, String format, String dateFormat, String initialTimeStamp, int timeStepinMilliSec,  Random timeGen, boolean randomizeTimeInBatch) {

        this.minPoints = minPoints;
        this.maxPoints = maxPoints;
        this.minPolygonHoles = minHoles;
        this.maxPolygonHoles = maxHoles;
        this.minGeometries = minGeometries;
        this.maxGeometries = maxGeometries;
        this.seriesVariance = seriesVariance;
        this.multiGeometryGenAlgorithm = multiGeometryGenAlgorithm;
        this.geometryGenAlgorithm = geometryGenAlgorithm;
        this.format = format;
        this.initialTimeStamp = initialTimeStamp;
        this.timeStepinMilliSec = timeStepinMilliSec;
        this.dateFormat = dateFormat;

    }


    @Override
    public DataStream<String> generate(DataStream<Tuple2<Integer, Long>> objIDStream, Envelope seriesBBox, SimpleDateFormat simpleDateFormat) {
        return objIDStream
                .keyBy(new HelperClass.objIDKeySelectorWithBatchID())
                .map(new AbstractRichMapFunction<Envelope>(Envelope.class, this.seriesBBox, Params.randomOption,
                        Params.seriesVariance, Params.hotspotVariance, Params.hotspotMean, Params.hotspotBBox) {

                    @Override
                    public String map(Tuple2<Integer, Long> objIDTuple) throws Exception {

                        Integer objID = objIDTuple.f0;
                        Long batchID = objIDTuple.f1;

                        LocalDateTime localDateTime = LocalDateTime.now();
                        long intervalNS = 0L;

                        if (lastTimestampVState != null && lastTimestampVState.value() != null) {
                            Duration duration = Duration.between(lastTimestampVState.value(), localDateTime);
                            try {
                                intervalNS = duration.toNanos();
                            }
                            catch (ArithmeticException e) {
                                e.printStackTrace();
                            }
                        }

                        Envelope geometryBBox;
                        if (lastGeometryVState != null && lastGeometryVState.value() != null) {
                            Envelope lastGeometryBBox = this.lastGeometryVState.value();

                            if(randomDistribution instanceof HotspotGaussianRandomWalk) {
                                int totalHotspots = ((HotspotGaussianRandomWalk) randomDistribution).getNumberOfHotspots();
                                int hotspotID = objID % totalHotspots;
                                hotspotGaussianRandomWalk.setHotspotID(hotspotID);

                                Coordinate minCoordinate = hotspotGaussianRandomWalk.next(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0,0, lastGeometryBBox.getMinX(), lastGeometryBBox.getMinY(), 0, intervalNS);
                                Coordinate maxCoordinate = hotspotGaussianRandomWalk.next(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0,0, lastGeometryBBox.getMaxX(), lastGeometryBBox.getMaxY(), 0, intervalNS);
                                geometryBBox = new Envelope(minCoordinate.getX(), maxCoordinate.getX(), minCoordinate.getY(), maxCoordinate.getY());

                            }else {
                                Coordinate minCoordinate = randomWalk.next(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0,0, lastGeometryBBox.getMinX(), lastGeometryBBox.getMinY(), 0, intervalNS);
                                Coordinate maxCoordinate = randomWalk.next(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0,0, lastGeometryBBox.getMaxX(), lastGeometryBBox.getMaxY(), 0, intervalNS);
                                geometryBBox = new Envelope(minCoordinate.getX(), maxCoordinate.getX(), minCoordinate.getY(), maxCoordinate.getY());
                            }
                        }
                        else if(randomDistribution instanceof HotspotDistribution && !(randomDistribution instanceof RandomWalk)){
                            int totalHotspots = ((HotspotDistribution) randomDistribution).getNumberOfHotspots();
                            int hotspotID = objID % totalHotspots;
                            hotspotDistribution.setHotspotID(hotspotID);

                            Coordinate minCoordinate = hotspotDistribution.nextOrFirst(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0,0);
                            Coordinate maxCoordinate = hotspotDistribution.nextOrFirst(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0,0);
                            geometryBBox = new Envelope(minCoordinate.getX(), maxCoordinate.getX(), minCoordinate.getY(), maxCoordinate.getY());
                        }
                        else {
                            Coordinate minCoordinate = randomDistribution.nextOrFirst(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0, 0);
                            Coordinate maxCoordinate = randomDistribution.nextOrFirst(seriesBBox.getMinX(), seriesBBox.getMaxX(), seriesBBox.getMinY(), seriesBBox.getMaxY(), 0, 0);
                            geometryBBox = new Envelope(minCoordinate.getX(), maxCoordinate.getX(), minCoordinate.getY(), maxCoordinate.getY());
                        }

                        int nPolygonPoints = HelperClass.getRandomIntInRange(minPoints, maxPoints);
                        int nPolygonHoles = HelperClass.getRandomIntInRange(minPolygonHoles, maxPolygonHoles);
                        int nGeometries = HelperClass.getRandomIntInRange(minGeometries, maxGeometries);
                        Geometry geometry = HelperClass.generateMultiPolygon(nPolygonPoints, nPolygonHoles, nGeometries, geometryBBox, multiGeometryGenAlgorithm,geometryGenAlgorithm);
                        if (format.equals("GeoJSON")) {
//                            return Serialization.generateGeometryJson(
//                                    geometry, objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString();

                            return Serialization.generateGeometryJson(
                                    geometry, objID,  HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)).toString();
                        }
                        else {
//                            return Serialization.generateGeometryWKT(
//                                    geometry, objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString();

                            return Serialization.generateGeometryWKT(
                                    geometry, objID,  HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)).toString();
                        }
                    }
                });
    }

}
