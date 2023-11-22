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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


public class IntStreamBroadcastManager1tuple extends RichFlatMapFunction<Tuple6<String, Long, Long, Long, Long, Set<Integer>>, Tuple2<Integer,Long>> implements Serializable {


    private  long countRows;

    private  HashSet<Integer> objIDrange = new HashSet<Integer>();

    private  HashSet<Integer> currentTrajIDs = new HashSet<Integer>();

    private  HashSet<Integer> removeTrajIDs = new HashSet<Integer>();

    private long numRows;

    private long batchID;

    private int receivedControlTuples = 0;
    private int totalControlTuples = 0;

    private int parallelism;


    public IntStreamBroadcastManager1tuple(long countRows, long numRows, int parallelism, HashSet<Integer> objIDrange, HashSet<Integer> currentTrajIDs, long batchID, double syncPercentage) throws Exception {
        this.numRows = numRows;
        this.objIDrange = objIDrange;
        this.currentTrajIDs = currentTrajIDs;
        this.countRows = countRows;
        this.batchID = batchID;
        this.parallelism = parallelism;
    }

    @Override
    public void flatMap(Tuple6<String, Long, Long, Long, Long, Set<Integer>> controlTuple, Collector<Tuple2<Integer,Long>> out) throws Exception {

        //      0                1                 2               3             4            5
        // "syncState", expectedBatchCount, totalBatchCount, currBatchCount, currbatchID, removeIDList
        totalControlTuples++;

        Set<Integer> removeIDs = controlTuple.f5;
        Long currBatchID = controlTuple.f4;
        Long currBatchCount = controlTuple.f3;
        Long totalBatchCount = controlTuple.f2;
        Long expectedBatchCount = controlTuple.f1;

        if (batchID == currBatchID.longValue())
        {receivedControlTuples++;}

        for (int remove_ID : removeIDs) {
            removeTrajIDs.add(remove_ID);
        }

        // generate next batch
        if (receivedControlTuples == parallelism) {

//            System.out.println(objIDrange.toString());
//            System.out.println(objIDrange.size());

            //remove objIDs (trajectories end)
            for (int remove_ID : removeTrajIDs) {
                currentTrajIDs.remove(remove_ID);
                objIDrange.remove(remove_ID);
            }

            removeTrajIDs.clear();
            receivedControlTuples = 0;
            batchID++;


            // generate objIDs
            for (int ID : objIDrange) {
                if ((numRows != -1) && (countRows >= numRows)) {
                    break;
                }
                out.collect(Tuple2.of(ID, batchID));
                countRows++;
            }

        }
    }


    @Override
    public void open(Configuration config) throws Exception {}


}