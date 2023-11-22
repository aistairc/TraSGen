package DataGen.timeSeriesGenerators.random;

import org.locationtech.jts.geom.Coordinate;

import java.util.Random;

public class UniformDistribution implements RandomDistribution {
    protected Random random;

    public UniformDistribution() {
        this.random = new Random();
    }

    public UniformDistribution(long seed) {
        this.random = new Random(seed);
    }

    @Override
    public Coordinate nextOrFirst(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ) {

        Coordinate c = new Coordinate();
        c.setX(rangeMinX + (rangeMaxX - rangeMinX) * random.nextDouble());
        c.setY(rangeMinY + (rangeMaxY - rangeMinY) * random.nextDouble());
        c.setZ(rangeMinZ + (rangeMaxZ - rangeMinZ) * random.nextDouble());

        return c;

    }
}
