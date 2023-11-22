package DataGen.timeSeriesGenerators.random;

import org.locationtech.jts.geom.Coordinate;

public interface RandomWalk extends RandomDistribution {
    public Coordinate next(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ, double lastXValue, double lastYValue, double lastZValue, long intervalNS);
}
