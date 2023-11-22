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
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;


public class IntStreamBroadcastManager extends RichFlatMapFunction<Tuple9<String, Integer, Long, Integer, Long, Long, Integer, Long, Long>, Tuple2<Integer,Long>> implements Serializable {

    private  Integer bcStateUpdatesCount = 0;

    private  long countRows;

    private  HashSet<Integer> objIDrange = new LinkedHashSet<Integer>();

    private  HashSet<Integer> currentTrajIDs = new LinkedHashSet<Integer>();

    private  HashSet<Integer> removeTrajIDs = new LinkedHashSet<Integer>();

    private Double syncPercentage;

    private int parallelism;
    private long numRows;

    private int cc = 0;

    private long batchID;

    private Long batchStateUpdatesCount = 0L;

    private Long expectedControlTuples;

    public IntStreamBroadcastManager(long countRows, long numRows, int parallelism, HashSet<Integer> objIDrange, HashSet<Integer> currentTrajIDs, long batchID, double syncPercentage) throws Exception {
        this.numRows = numRows;
        this.parallelism = parallelism;
        this.objIDrange = objIDrange;
        this.currentTrajIDs = currentTrajIDs;
        this.countRows = countRows;
        this.batchID = batchID;
        this.syncPercentage = syncPercentage;
        this.expectedControlTuples = (long) Math.ceil((currentTrajIDs.size() * parallelism * 2) * syncPercentage/100.0);

    }

    @Override
    public void flatMap(Tuple9<String, Integer, Long, Integer, Long, Long, Integer, Long, Long> bcStateUpdate, Collector<Tuple2<Integer,Long>> out) throws Exception {
        bcStateUpdatesCount++;


        Long objID = bcStateUpdate.f7;
        int trajStatus = bcStateUpdate.f6.intValue();
        long recvBatchID = bcStateUpdate.f8.longValue();

//        System.out.println("batchStateUpdatesCount: " + batchStateUpdatesCount);
//        System.out.println("bcStateUpdatesCount: " + bcStateUpdatesCount);

        if (recvBatchID == batchID) {
            this.batchStateUpdatesCount++;
        }

        // add objID (new trajectory)
        if (trajStatus == 1) {
            currentTrajIDs.add(objID.intValue());
//            this.expectedControlTuples = (long) Math.ceil((currentTrajIDs.size() * parallelism * 2) * syncPercentage/100.0);
        }

        // reset count to 0 if count exceeds batchsize
//        if (bcStateUpdatesCount > expectedControlTuples) {
//            bcStateUpdatesCount = 1;
//        }

        // reset batch count to 0 if count exceeds batchsize  #deprecated
//        if (batchStateUpdatesCount.longValue() > expectedControlTuples.longValue()) {
//            batchStateUpdatesCount = 1L;
//        }

        // add objID to remove list
        if (trajStatus == -1) {
            removeTrajIDs.add(objID.intValue());
        }
        System.out.println("Total bcStateUpdatesCount: " + bcStateUpdatesCount);
        System.out.println("batchStateUpdatesCount: " + batchStateUpdatesCount);
        // generate next batch
        if ((this.syncPercentage.equals(0.0)) || batchStateUpdatesCount.equals(expectedControlTuples)) {
            System.out.println("EXPECTED: " + expectedControlTuples);
//            System.out.println("batchStateUpdatesCount: " + batchStateUpdatesCount);


            batchID++;


            //remove objIDs (trajectories end)
            for (int remove_ID : removeTrajIDs) {
                currentTrajIDs.remove(remove_ID);
                objIDrange.remove(remove_ID);
                System.out.println("Current IDS: " + Arrays.toString(objIDrange.toArray()));
            }
            removeTrajIDs.clear();

            for (int ID : objIDrange) {
                if ((numRows != -1) && (countRows >= numRows)) {
                    break;
                }
                out.collect(Tuple2.of(ID, batchID));
                countRows++;
            }
            this.expectedControlTuples = (long) Math.floor((this.currentTrajIDs.size() * this.parallelism * 2) * this.syncPercentage/100.0);
            this.batchStateUpdatesCount = 0L;
        }

    }


    @Override
    public void open(Configuration config) throws Exception {}


}