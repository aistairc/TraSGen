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
