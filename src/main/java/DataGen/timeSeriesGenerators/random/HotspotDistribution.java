package DataGen.timeSeriesGenerators.random;

public interface HotspotDistribution extends RandomDistribution {
    public void setHotspotID(int hotspotID);
    public int getNumberOfHotspots();
}
