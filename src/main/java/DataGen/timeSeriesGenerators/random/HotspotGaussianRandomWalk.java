package DataGen.timeSeriesGenerators.random;

import DataGen.utils.HelperClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;

import java.util.List;
import java.util.Random;

public class HotspotGaussianRandomWalk extends HotspotGaussianDistribution implements RandomWalk {
    protected Random random;
    protected List<Coordinate> hotspotMean;
    protected List<Coordinate> hotspotVar;
    protected List<Tuple2<Coordinate, Coordinate>> hotspotBBox;
    protected double variance;

    public HotspotGaussianRandomWalk(List<Coordinate> hotspotMean, List<Coordinate> hotspotVar, List<Tuple2<Coordinate, Coordinate>> hotspotBBox, double variance){
        super(hotspotMean, hotspotVar);
        this.random = new Random();
        if(hotspotMean == null || hotspotVar == null || variance <= 0) {
            throw new IllegalArgumentException("Invalid hotspot variance, mean or series variance");
        }else{
            this.hotspotMean = hotspotMean;
            this.hotspotVar = hotspotVar;
            this.variance = variance;
            this.hotspotBBox = hotspotBBox;
        }
    }

    public HotspotGaussianRandomWalk(long seed, List<Coordinate> hotspotMean, List<Coordinate> hotspotVar, List<Tuple2<Coordinate, Coordinate>> hotspotBBox, double variance){
        super(seed, hotspotMean, hotspotVar);
        this.random = new Random();
        if(hotspotMean == null || hotspotVar == null || variance <= 0) {
            throw new IllegalArgumentException("Invalid hotspot variance, mean or series variance");
        }else{
            this.hotspotMean = hotspotMean;
            this.hotspotVar = hotspotVar;
            this.variance = variance;
            this.hotspotBBox = hotspotBBox;
        }
    }

    @Override
    public Coordinate next(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ, double lastXValue, double lastYValue, double lastZValue, long intervalNS) {
        double x, y, z = 0;
        double diff;
        Tuple2<Coordinate, Coordinate> hotspotBBoxCoordinates = hotspotBBox.get(hotspotID);
        Coordinate minCoordinate = hotspotBBoxCoordinates.f0;
        Coordinate maxCoordinate = hotspotBBoxCoordinates.f1;

        do{
            diff =  random.nextGaussian() * variance;
            x = lastXValue + diff;
        }while(!HelperClass.withinRange(x, minCoordinate.getX(), maxCoordinate.getX()));

        do{
            diff =  random.nextGaussian() * variance;
            y = lastYValue + diff;
        }while(!HelperClass.withinRange(y, minCoordinate.getY(), maxCoordinate.getY()));

        if(rangeMinZ != rangeMaxZ) {
            do {
                diff =  random.nextGaussian() * variance;
                z = lastZValue + diff;
            } while (!HelperClass.withinRange(z, minCoordinate.getZ(), maxCoordinate.getZ()));
        }

        return new Coordinate(x, y, z);
    }

}
