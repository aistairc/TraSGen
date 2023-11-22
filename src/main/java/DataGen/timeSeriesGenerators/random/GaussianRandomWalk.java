package DataGen.timeSeriesGenerators.random;

import org.locationtech.jts.geom.Coordinate;

public class GaussianRandomWalk extends UniformDistribution implements RandomWalk {
    protected double variance;

    public GaussianRandomWalk(double variance){
        super();
        if(variance <= 0) {
            throw new IllegalArgumentException("Not support such a variance");
        }else{
            this.variance = variance;
        }
    }

    public GaussianRandomWalk(long seed, double variance){
        super(seed);
        if(variance <= 0) {
            throw new IllegalArgumentException("Not support such a variance");
        }else{
            this.variance = variance;
        }
    }

    @Override
    public Coordinate next(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ, double lastXValue, double lastYValue, double lastZValue, long intervalNS) {
        double diff;
        diff =  random.nextGaussian() * variance;
        double nextX = lastXValue + diff;
        diff =  random.nextGaussian() * variance;
        double nextY = lastYValue + diff;
        diff =  random.nextGaussian() * variance;
        double nextZ = lastZValue + diff;

        return new Coordinate(nextX, nextY, nextZ);
    }
}
