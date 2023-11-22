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

package DataGen.timeSeriesGenerators.network;

import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

public interface NetworkDistribution extends Serializable {
    Coordinate next(Coordinate lastCoordinateLngLat, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs,  GeodeticCalculator gc);
//    Coordinate next(Coordinate lastCoordinateLngLat, Double azimuthInDecimalDegrees, Double distanceInMeters, CoordinateReferenceSystem crs);
}
