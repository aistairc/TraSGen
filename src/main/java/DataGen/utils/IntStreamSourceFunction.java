package DataGen.utils;

import DataGen.inputParameters.Params;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.Serializable;

public abstract class IntStreamSourceFunction<Integer> extends RichSourceFunction<Integer> implements Serializable {
    protected int minObjID;
    protected int maxObjID;
    protected long numRows;
    protected int consecutiveTrajTuplesIntervalMilliSec;


    public IntStreamSourceFunction(int minObjID, int maxObjID, long numRows, int consecutiveTrajTuplesIntervalMilliSec){
        this.minObjID = minObjID;
        this.maxObjID = maxObjID;
        this.numRows = numRows;
        this.consecutiveTrajTuplesIntervalMilliSec = consecutiveTrajTuplesIntervalMilliSec;

    }
}
