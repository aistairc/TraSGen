package DataGen.timeSeriesGenerators.random;

import org.locationtech.jts.geom.Coordinate;

public class SimpleRandomWalk extends UniformDistribution implements RandomWalk {
    protected double diff;

    public SimpleRandomWalk(double diff){
        super();
        if(diff <= 0) {
            throw new IllegalArgumentException("Not support such a diff");
        }else{
            this.diff = diff;
        }
    }

    public SimpleRandomWalk(long seed, double diff){
        super(seed);
        if(diff <= 0) {
            throw new IllegalArgumentException("Not support such a diff");
        }else{
            this.diff = diff;
        }
    }

    @Override
    public Coordinate next(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ, double lastXValue, double lastYValue, double lastZValue, long intervalNS) {
        int plusMinus;
        plusMinus = random.nextBoolean() ? 1 : -1;
        double nextX = lastXValue + (plusMinus*diff);
        plusMinus = random.nextBoolean() ? 1 : -1;
        double nextY = lastYValue + (plusMinus*diff);
        plusMinus = random.nextBoolean() ? 1 : -1;
        double nextZ = lastZValue + (plusMinus*diff);

        return new Coordinate(nextX, nextY, nextZ);
    }
}
