package DataGen.timeSeriesGenerators.random;

import DataGen.utils.HelperClass;
import org.locationtech.jts.geom.Coordinate;

import java.util.List;
import java.util.Random;

public class HotspotGaussianDistribution implements HotspotDistribution{
    protected Random random;
    protected List<Coordinate> hotspotMean;
    protected List<Coordinate> hotspotVar;
    protected int hotspotID;

    public HotspotGaussianDistribution(List<Coordinate> hotspotMean, List<Coordinate> hotspotVar){
        super();
        this.random = new Random();
        if(hotspotMean == null || hotspotVar == null) {
            throw new IllegalArgumentException("Invalid hotspot variance or mean");
        }else{
            this.hotspotMean = hotspotMean;
            this.hotspotVar = hotspotVar;
        }
    }

    public HotspotGaussianDistribution(long seed, List<Coordinate> hotspotMean, List<Coordinate> hotspotVar){
        this.random = new Random(seed);
        if(hotspotMean == null || hotspotVar == null) {
            throw new IllegalArgumentException("Invalid hotspot variance or mean");
        }else{
            this.hotspotMean = hotspotMean;
            this.hotspotVar = hotspotVar;
        }
    }

    @Override
    public void setHotspotID(int hotspotID) {
        this.hotspotID = hotspotID;
    }

    @Override
    public int getNumberOfHotspots() {
        return hotspotMean.size();
    }

    @Override
    public Coordinate nextOrFirst(double rangeMinX, double rangeMaxX, double rangeMinY, double rangeMaxY, double rangeMinZ, double rangeMaxZ) {

        double x, y, z = 0;

        do{
            x = hotspotMean.get(hotspotID).getX() + random.nextGaussian() * hotspotVar.get(hotspotID).getX();
        }while(!HelperClass.withinRange(x, rangeMinX, rangeMaxX));

        do{
            y = hotspotMean.get(hotspotID).getY() + random.nextGaussian() * hotspotVar.get(hotspotID).getY();
        }while(!HelperClass.withinRange(y, rangeMinY, rangeMaxY));

        if(rangeMinZ != rangeMaxZ) {
            do {
                z = hotspotMean.get(hotspotID).getZ() + random.nextGaussian() * hotspotVar.get(hotspotID).getZ();
            } while (!HelperClass.withinRange(z, rangeMinZ, rangeMaxZ));
        }

        return new Coordinate(x, y, z);
    }



}