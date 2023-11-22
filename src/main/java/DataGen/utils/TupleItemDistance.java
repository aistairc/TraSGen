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

import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;

public class TupleItemDistance implements ItemDistance {


    @Override
    public double distance(ItemBoundable item1, ItemBoundable item2) {
        Tuple2<Point,String> tupleA =  (Tuple2<Point,String>) item1.getItem();
        Tuple2<Point,String> tupleB =  (Tuple2<Point,String>) item2.getItem();

        Geometry g1 = tupleA.f0;
        Geometry g2 = tupleB.f0;

        return g1.distance(g2);
    }
}
