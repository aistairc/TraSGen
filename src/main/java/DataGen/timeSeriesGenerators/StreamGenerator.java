package DataGen.timeSeriesGenerators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.text.SimpleDateFormat;

public interface StreamGenerator extends Serializable {
    final int NUM_OF_POINTS = 3;
    final int NUM_OF_POLYGON_HOLES = 0;
    final int NUM_OF_GEOMETRIES = 2;
    final double SERIES_VARIANCE = 0.005;
    final int ROAD_CAPACITY = 5;

//    public DataStream<String> generate(DataStream<Integer> objIDStream, Envelope seriesBBox, SimpleDateFormat simpleDateFormat) throws Exception;
//
////        public DataStream<String> generate(DataStream<Tuple2<Integer,Long>> objIDStream, Envelope seriesBBox, SimpleDateFormat simpleDateFormat) throws Exception;


    public DataStream<String> generate(DataStream<Tuple2<Integer,Long>> objIDStream, Envelope seriesBBox, SimpleDateFormat simpleDateFormat) throws Exception;

}
