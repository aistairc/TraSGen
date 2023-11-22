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
import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class PointStreamGenerator implements StreamGenerator {
    private String outputFormat = "GeoJSON";
    private Envelope seriesBBox = null;
    //private int totalHotspots;

    private String initialTimeStamp = null;

    private String dateFormat = null;

    private int timeStepinMilliSec = 0;


    boolean randomizeTimeInBatch;

    public PointStreamGenerator( Envelope seriesBBox, String outputFormat, String dateFormat, String initialTimeStamp, int timeStepinMilliSec, boolean randomizeTimeInBatch){
        this.outputFormat = outputFormat;
        this.seriesBBox = seriesBBox;
        this.initialTimeStamp = initialTimeStamp;
        this.timeStepinMilliSec = timeStepinMilliSec;
        this.dateFormat = dateFormat;
        this.randomizeTimeInBatch = randomizeTimeInBatch;
    }

    /*
    public PointStreamGenerator(RandomDistribution randomDistribution, String format, int totalHotspots){
        this.randomDistribution = randomDistribution;
        this.totalHotspots = totalHotspots;
        this.format = format;
    }
    */

    @Override
    public DataStream<String> generate(DataStream<Tuple2<Integer, Long>> objIDStream, Envelope seriesBBox, SimpleDateFormat simpleDateFormat) {
        return objIDStream
                .keyBy(new HelperClass.objIDKeySelectorWithBatchID())
                .map(new AbstractRichMapFunction<Coordinate>(Coordinate.class, this.seriesBBox, Params.randomOption,
                        Params.seriesVariance, Params.hotspotVariance, Params.hotspotMean, Params.hotspotBBox) {

//                    assert (streamGenerator != null) : "streamGenerator is null";

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

                        Coordinate c;
                        if(lastGeometryVState != null && lastGeometryVState.value() != null){
                            Coordinate lastPoint = this.lastGeometryVState.value();
                            
                            if(this.randomDistribution instanceof HotspotGaussianRandomWalk) {
                                int totalHotspots = ((HotspotGaussianRandomWalk) this.randomDistribution).getNumberOfHotspots();
                                int hotspotID = objID % totalHotspots;
                                hotspotGaussianRandomWalk.setHotspotID(hotspotID);
                                c = hotspotGaussianRandomWalk.next(this.seriesBBox.getMinX(), this.seriesBBox.getMaxX(), this.seriesBBox.getMinY(), this.seriesBBox.getMaxY(), 0, 0, lastPoint.getX(), lastPoint.getY(), 0, intervalNS);

                            }else {
                                c = this.randomWalk.next(this.seriesBBox.getMinX(), this.seriesBBox.getMaxX(), this.seriesBBox.getMinY(), this.seriesBBox.getMaxY(), 0, 0, lastPoint.getX(), lastPoint.getY(), 0, intervalNS);
                            }
                        }
                        else if(this.randomDistribution instanceof HotspotDistribution && !(this.randomDistribution instanceof RandomWalk)){
                            int totalHotspots = ((HotspotDistribution) randomDistribution).getNumberOfHotspots();
                            int hotspotID = objID % totalHotspots;
                            hotspotDistribution.setHotspotID(hotspotID);
                            c = hotspotDistribution.nextOrFirst(this.seriesBBox.getMinX(), this.seriesBBox.getMaxX(), this.seriesBBox.getMinY(), this.seriesBBox.getMaxY(), 0, 0);
                        }
                        else {
                            c = this.randomDistribution.nextOrFirst(this.seriesBBox.getMinX(), this.seriesBBox.getMaxX(), this.seriesBBox.getMinY(), this.seriesBBox.getMaxY(), 0, 0);
                        }

                        if (lastGeometryVState != null) {
                            lastGeometryVState.update(c);
                            lastTimestampVState.update(localDateTime);
                        }



                        if (outputFormat.equals("GeoJSON")) {
//                            return Serialization.generatePointJson(
//                                    c.x, c.y, objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime))).toString();
//
                            return Serialization.generatePointJson(c.x, c.y, objID, batchID,
                                    HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch)).toString();
                        }
                        else {
//                            return Serialization.generateGeometryWKT(
//                                    HelperClass.generatePoint(c), objID, simpleDateFormat.format(HelperClass.localDateTimeToDate(localDateTime)));

                            return Serialization.generateGeometryWKT(
                                    HelperClass.generatePoint(c), objID, batchID,  HelperClass.TimeStamp(dateFormat, initialTimeStamp, timeStepinMilliSec, batchID, timeGen, randomizeTimeInBatch));

                        }
                    }
                });
    }

}