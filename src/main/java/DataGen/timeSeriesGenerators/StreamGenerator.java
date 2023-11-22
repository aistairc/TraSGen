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


    public DataStream<String> generate(DataStream<Tuple2<Integer,Long>> objIDStream, SimpleDateFormat simpleDateFormat) throws Exception;

}
