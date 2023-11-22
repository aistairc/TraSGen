package DataGen.timeSeriesGenerators.random;

import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;

public interface RandomDistribution extends Serializable {
    public Coordinate nextOrFirst(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ);
}
