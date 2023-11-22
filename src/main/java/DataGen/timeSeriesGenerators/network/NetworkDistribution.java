package DataGen.timeSeriesGenerators.network;

import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

public interface NetworkDistribution extends Serializable {
    Coordinate next(Coordinate lastCoordinateLngLat, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs,  GeodeticCalculator gc);
//    Coordinate next(Coordinate lastCoordinateLngLat, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs);
}
