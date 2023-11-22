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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class IntegerStream {

    //private ArrayList<Integer> streamIDList = new ArrayList<Integer>();
    private final int minObjID;
    private final int maxObjID;
    private final long numRows;
    StreamExecutionEnvironment env;


    public IntegerStream(StreamExecutionEnvironment env, int minObjID, int maxObjID, long numRows){
        this.minObjID = minObjID;
        this.maxObjID = maxObjID;
        this.numRows = numRows;
        this.env = env;
    }

    public DataStream<Integer> generateRoundRobin(){
        // "vehicle" // Round Robin Data Generation
        return env.addSource(new IntStreamSourceFunction<Integer>(this.minObjID, this.maxObjID, this.numRows, Params.consecutiveTrajTuplesIntervalMilliSec) {
            private long count = 0L;
            private int networkObjID = minObjID;
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {

                int totalObjIDs =  maxObjID - minObjID + 1;
                int objIDCounter = 0;
                // boolean startTimeUpdated = false;
                // start time
                long startTime = System.nanoTime();
                while(count < numRows || numRows < 0){ // numRows -1 for infinite number of rows
                    // round-robin ID generator for network based approach
                    int x;

                     x = networkObjID++;
                     if(networkObjID > maxObjID){
                         networkObjID = minObjID;
                     }
                     sourceContext.collect(x);
                     objIDCounter++;

                    //System.out.println("x, objIDCounter: " + x + ", " + objIDCounter);
                    // If consecutiveTrajTuplesIntervalSec passed during the loop of objID generation
                    //                     if((System.nanoTime() - startTime)/1E9 >= Params.consecutiveTrajTuplesIntervalSec){
                    //                         startTime = System.nanoTime();
                    //                         startTimeUpdated = true;
                    //                     }

                     if(objIDCounter >= totalObjIDs){
                         objIDCounter = 0;
                         // loop to add pause in data generation
                         long endTime;
                         do{
                             endTime = System.nanoTime();
                         }while((endTime - startTime)/1E6 < this.consecutiveTrajTuplesIntervalMilliSec);
                         startTime = System.nanoTime();

                         //if(!startTimeUpdated) {
                         //    startTime = System.nanoTime();
                         //}
                     }

                     if(numRows > 0) {
                        count++;
                     }
                }
            }
            @Override
            public void cancel() {
            }
        }).name("oID Generator Async");
    }


    public DataStream<Tuple2<Integer, Long>> generateRoundRobinWithBatchID(){
        // "vehicle" // Round Robin Data Generation
        return env.addSource(new IntStreamSourceFunction<Tuple2<Integer, Long>>(this.minObjID, this.maxObjID, this.numRows, Params.consecutiveTrajTuplesIntervalMilliSec) {
            private long count = 0L;
            private long batchID = 1L;
            private int networkObjID = minObjID;
            @Override
            public void run(SourceContext<Tuple2<Integer, Long>> sourceContext) throws Exception {

                int totalObjIDs =  maxObjID - minObjID + 1;
                int objIDCounter = 0;
                // boolean startTimeUpdated = false;

                while(count < numRows || numRows < 0){ // numRows -1 for infinite number of rows
                    // round-robin ID generator for network based approach
                    int x;

                    x = networkObjID++;
                    if(networkObjID > maxObjID){
                        networkObjID = minObjID;

                    }
                    sourceContext.collect(Tuple2.of(x, batchID));
                    objIDCounter++;

                    //System.out.println("x, objIDCounter: " + x + ", " + objIDCounter);
                    // If consecutiveTrajTuplesIntervalSec passed during the loop of objID generation
                    //                     if((System.nanoTime() - startTime)/1E9 >= Params.consecutiveTrajTuplesIntervalSec){
                    //                         startTime = System.nanoTime();
                    //                         startTimeUpdated = true;
                    //                     }

                    if(objIDCounter >= totalObjIDs){
                        objIDCounter = 0;
                        batchID++;
                        // loop to add pause in data generation
                        long startTime = System.nanoTime();
                        long endTime;
                        do{
                            endTime = System.nanoTime();
                        }while((endTime - startTime)/1E6 < this.consecutiveTrajTuplesIntervalMilliSec);
                        startTime = System.nanoTime();


                        //if(!startTimeUpdated) {
                        //    startTime = System.nanoTime();
                        //}
                    }

                    if(numRows > 0) {
                        count++;
                    }
                }
            }
            @Override
            public void cancel() {
            }
        }).name("oID Generator Async");
    }



    public DataStream<Integer> generateRandom(){
        // "random" // Random data generation
        return env.addSource(new IntStreamSourceFunction<Integer>(this.minObjID, this.maxObjID, this.numRows, Params.consecutiveTrajTuplesIntervalMilliSec) {
            private long count = 0L;

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while(count < numRows || numRows < 0){ // numRows
                    // for infinite number of rows
                    int x;

                    x = HelperClass.getRandomIntInRange(minObjID, maxObjID + 1);
                    sourceContext.collect(x);
                    if(numRows > 0) {
                        count++;
                    }
                }
            }
            @Override
            public void cancel() {
            }
        }).name("oID Generator Async Random");
    }
}
