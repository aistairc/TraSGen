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

import DataGen.inputParameters.Params;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.*;
import java.util.stream.Collectors;

public class IntegerStreamBroadcast {

    private  HashSet<Integer> objIDrange = new LinkedHashSet<Integer>();

    private  HashSet<Integer> currentTrajIDs = new LinkedHashSet<Integer>();
    private final int minObjID;
    private final int maxObjID;
    private final long numRows;

    private  long countRows = 0;

    StreamExecutionEnvironment env;
    private Properties kafkaProperties = null;
    private long count = 0L;
    private int ptrID = 0;

    private int totalObjIDs;
    private long batchID = 1;
    private  HashSet<Tuple2<Integer,Long>> initializerBatch = new LinkedHashSet<Tuple2<Integer,Long>>();

    private Integer objID;

    //TODO Only supports roundrobin, expand to random
    public IntegerStreamBroadcast(StreamExecutionEnvironment env, int minObjID, int maxObjID, long numRows, Properties kafkaProperties) {
        this.minObjID = minObjID;
        this.maxObjID = maxObjID;
        this.numRows = numRows;
        this.env = env;
        this.kafkaProperties = kafkaProperties;
        this.totalObjIDs =  maxObjID - minObjID + 1;
//
        for(int i = minObjID ; i < totalObjIDs + 1 ; i++)
            this.objIDrange.add(i);

        for(int ID : objIDrange) {
            if ((numRows != -1) && (countRows >= this.numRows))  break;
            this.currentTrajIDs.add(ID);
            countRows++;
        }

        //create batchID tuples
        for(int ID : currentTrajIDs) {
            this.initializerBatch.add(Tuple2.of(ID,batchID));
        }

    }

    public DataStream<Tuple2<Integer,Long>> generateRoundRobin() throws Exception {
        //TODO convert to Integer object ID to long, set parallelism accordingly

        int parallelism = Params.parallelism;
        double syncPercentage = Params.syncPercentage;

        DataStream<Tuple2<Integer,Long>> intializeObjIDStream = this.env.fromCollection(initializerBatch).name("oID Generator Initializer");;


        DataStream<String> controlTupleString = this.env.addSource(new FlinkKafkaConsumer<>("BroadcastStateUpdate", new SimpleStringSchema(), this.kafkaProperties)).name("Control Tuples Source");
        //      0                1                 2               3             4            5
        // "syncState", expectedBatchCount, totalBatchCount, currBatchCount, currbatchID, removeIDList
        DataStream<Tuple6<String, Long, Long, Long, Long, Set<Integer>>> controlTuples = controlTupleString.map(new MapFunction<String, Tuple6<String, Long, Long, Long, Long, Set<Integer>>>() {
            @Override
            public Tuple6<String, Long, Long, Long, Long, Set<Integer>> map(String str) throws Exception {
                String[] temp = str.split(",");
                String[] removeIDString= temp[5].split("-");
                int[] removeIDsArray  = Arrays.stream(removeIDString).mapToInt(Integer::parseInt).toArray();
                Set<Integer> removeIDs = Arrays.stream(removeIDsArray).boxed().collect(Collectors.toSet());
                return Tuple6.of(temp[0], Long.parseLong(temp[1]), Long.parseLong(temp[2]), Long.parseLong(temp[3]), Long.parseLong(temp[4]), removeIDs);
            }
        });
//        controlTuples.print();

        DataStream<Tuple2<Integer,Long>> objIDs = controlTuples.flatMap(new IntStreamBroadcastManager1tuple(countRows, numRows, parallelism, objIDrange, currentTrajIDs, batchID, syncPercentage)).setParallelism(1).name("oID Generator");
//
        return intializeObjIDStream.union(objIDs);
    }
}
