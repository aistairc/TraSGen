package DataGen.utils;

import org.locationtech.jts.geom.Coordinate;

public class CoordinateRadian {
    private Coordinate coordinate;
    private double radian;
    private int count;

    public CoordinateRadian(Coordinate coordinate, double radian, int count) {
        this.coordinate = coordinate;
        this.radian = radian;
        this.count = count;
    }

    public Coordinate getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(Coordinate coordinate) {
        this.coordinate = coordinate;
    }

    public double getRadian() {
        return radian;
    }

    public void setRadian(double radian) {
        this.radian = radian;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
