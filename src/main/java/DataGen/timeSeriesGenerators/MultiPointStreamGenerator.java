
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

public class MultiPointStreamGenerator implements StreamGenerator {
    private final int minGeometries;
    private final int maxGeometries;

    private final double seriesVar;
    private int multiGeometryGenAlgorithm = 0;
    private String format = "GeoJSON";
    private String initialTimeStamp = null;
    private String dateFormat = null;
    private int timeStepinMilliSec = 0;
    private Envelope seriesBBox;

    private Random timeGen = null;

    boolean randomizeTimeInBatch;

    public MultiPointStreamGenerator(){

        this.minGeometries = NUM_OF_GEOMETRIES;
        this.maxGeometries = NUM_OF_GEOMETRIES;
        this.seriesVar = SERIES_VARIANCE;
    }

    public MultiPointStreamGenerator(int minGeometries, int maxGeometries){
        this.minGeometries = minGeometries;
        this.maxGeometries = maxGeometries;
        this.seriesVar = SERIES_VARIANCE;
    }

    public MultiPointStreamGenerator( double seriesVar){
        this.minGeometries = NUM_OF_GEOMETRIES;
        this.maxGeometries = NUM_OF_GEOMETRIES;
        this.seriesVar = seriesVar;
    }

    public MultiPointStreamGenerator(int minGeometries, int maxGeometries, double seriesVar,
                                     int multiGeometryGenAlgorithm, String format, String dateFormat, String initialTimeStamp, int timeStepinMilliSec, Random timeGen, boolean randomizeTimeInBatch) {

        this.minGeometries = minGeometries;
        this.maxGeometries = maxGeometries;
        this.seriesVar = seriesVar;
        this.multiGeometryGenAlgorithm = multiGeometryGenAlgorithm;
        this.format = format;
        this.initialTimeStamp = initialTimeStamp;
        this.timeStepinMilliSec = timeStepinMilliSec;
        this.dateFormat = dateFormat;
        this.timeGen = timeGen;
        this.randomizeTimeInBatch = randomizeTimeInBatch;
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


                        if (lastGeometryVState != null) {
                            lastGeometryVState.update(geometryBBox);
                            lastTimestampVState.update(localDateTime);
                        }

                        int nMultiPointGeometries = HelperClass.getRandomIntInRange(minGeometries, maxGeometries);
                        Geometry geometry = HelperClass.generateMultiPoint(nMultiPointGeometries, geometryBBox, multiGeometryGenAlgorithm);
                        if (format.equals("GeoJSON")) {
//                            return Serialization.generateGeometryJson(
//                                    geometry, objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString();

                            return Serialization.generateGeometryJson(
                                    geometry, objID, HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)).toString();
                        }
                        else {
//                            return Serialization.generateGeometryWKT(
//                                    geometry, objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString();

                            return Serialization.generateGeometryWKT(
                                    geometry, objID, HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch));
                        }
                    }
                });
    }

}