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
