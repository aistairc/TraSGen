package DataGen.timeSeriesGenerators.random;

import org.locationtech.jts.geom.Coordinate;

import java.util.Random;

public class  GaussianDistribution implements RandomDistribution {

    protected Random random;

    public GaussianDistribution(){
        this.random = new Random();
    }
    public GaussianDistribution(long seed){
        this.random = new Random(seed);
    }

    @Override
    public Coordinate nextOrFirst(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ) {

        Coordinate c = new Coordinate();
        c.setX(rangeMinX + (rangeMaxX - rangeMinX) * random.nextGaussian());
        c.setY(rangeMinY + (rangeMaxY - rangeMinY) * random.nextGaussian());
        c.setZ(rangeMinZ + (rangeMaxZ - rangeMinZ) * random.nextGaussian());

        return c;
    }
}
