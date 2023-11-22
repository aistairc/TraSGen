/*
 * Copyright 2023 Data Platform Research Team, AIRC, AIST, Japan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package DataGen.utils;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.Arrays;

public class SpatialFunctions implements Serializable {

/**
    Uses Haversine method as its base.
 */

    public static double getDistanceInMeters(Coordinate firstCoordinate, Coordinate secondCoordinate, CoordinateReferenceSystem crs,  GeodeticCalculator gc) {

        double distance = 0.0;

        try {
//            distance = JTS.orthodromicDistance(firstCoordinate,secondCoordinate,crs); //caused low throughput with parallelism
            //Optimized
            gc.setStartingPosition(JTS.toDirectPosition(firstCoordinate, crs));
            gc.setDestinationPosition(JTS.toDirectPosition(secondCoordinate, crs));
            distance =  gc.getOrthodromicDistance();

        } catch (TransformException e) {
            e.printStackTrace();
        }

        return distance;
    }

    // Returns the azimuth in decimal degrees from -180° to +180°.
    public static double getAzimuthInDecimalDegrees(Coordinate startingCoordinate, Coordinate destinationCoordinate, CoordinateReferenceSystem crs, GeodeticCalculator gc) {
//        public static double getAzimuthInDecimalDegrees(Coordinate startingCoordinate, Coordinate destinationCoordinate, CoordinateReferenceSystem crs) {

        double angle = 0.0;

        try {
            // the following code is based on JTS.orthodromicDistance( start, end, crs )
//            GeodeticCalculator gc = new GeodeticCalculator(crs);
            gc.setStartingPosition(JTS.toDirectPosition(startingCoordinate, crs));
            gc.setDestinationPosition(JTS.toDirectPosition(destinationCoordinate, crs));
            angle = gc.getAzimuth();
        } catch (TransformException e) {
            e.printStackTrace();
        }

        return angle;

        /*
        int totalmeters = (int) distance;
        int km = totalmeters / 1000;
        int meters = totalmeters - (km * 1000);
        float remaining_cm = (float) (distance - totalmeters) * 10000;
        remaining_cm = Math.round(remaining_cm);
        float cm = remaining_cm / 100;
        System.out.println("Distance = " + km + "km " + meters + "m " + cm + "cm");
        */
    }

    public static Coordinate


//    getDistantLocation(Coordinate startingCoordinate, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs) {
        getDistantLocation(Coordinate startingCoordinate, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs,  GeodeticCalculator calc) {

//        if (crs == null) {
//            crs = Params.coordinateReferenceSystem;
//        }

//        GeodeticCalculator calc = new GeodeticCalculator(crs);
        // lon/lat
        calc.setStartingGeographicPoint(startingCoordinate.x, startingCoordinate.y);
        calc.setDirection(azimuthInDecimalDegrees /* azimuth */, distanceInMeters /* distance */);
        Point2D dest = calc.getDestinationGeographicPoint();

        return new Coordinate(dest.getX(), dest.getY());
    }

    public static void copy(final Coordinate point, final double[] ordinates) {

        switch (ordinates.length) {
            default:
                Arrays.fill(ordinates, 3, ordinates.length, Double.NaN); // Fall through

            case 3:
                ordinates[2] = point.getZ(); // Fall through

            case 2:
                ordinates[1] = point.y; // Fall through

            case 1:
                ordinates[0] = point.x; // Fall through

            case 0:
                break;
        }
    }
}
