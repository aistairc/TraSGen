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