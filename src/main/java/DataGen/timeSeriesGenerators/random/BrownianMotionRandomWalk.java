package DataGen.timeSeriesGenerators.random;

import org.locationtech.jts.geom.Coordinate;

public class BrownianMotionRandomWalk extends UniformDistribution implements RandomWalk {
    protected double variance;

    public BrownianMotionRandomWalk(double variance){
        super();
        if(variance <= 0) {
            throw new IllegalArgumentException("Not support such a variance");
        }else{
            this.variance = variance;
        }
    }

    public BrownianMotionRandomWalk(long seed, double variance){
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
        diff =  random.nextGaussian() * intervalNS;
        double nextX = lastXValue + diff;
        diff =  random.nextGaussian() * intervalNS;
        double nextY = lastYValue + diff;
        diff =  random.nextGaussian() * intervalNS;
        double nextZ = lastZValue + diff;

        return new Coordinate(nextX, nextY, nextZ);
    }
}
