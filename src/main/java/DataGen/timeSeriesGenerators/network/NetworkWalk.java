package DataGen.timeSeriesGenerators.network;

import DataGen.utils.SpatialFunctions;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

public class NetworkWalk implements NetworkDistribution, Serializable {

    public NetworkWalk(){}

    @Override
    public Coordinate next(Coordinate lastCoordinateLngLat, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs, GeodeticCalculator gc) {
        return SpatialFunctions.getDistantLocation(lastCoordinateLngLat, azimuthInDecimalDegrees, distanceInMeters, crs, gc);
    }
}
//    @Override
//    public Coordinate next(Coordinate lastCoordinateLngLat, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs) {
//        return SpatialFunctions.getDistantLocation(lastCoordinateLngLat, azimuthInDecimalDegrees, distanceInMeters, crs);
//    }
//}
