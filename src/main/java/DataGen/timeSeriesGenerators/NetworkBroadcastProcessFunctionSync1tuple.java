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

import DataGen.timeSeriesGenerators.network.NetworkDistribution;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.geotools.referencing.GeodeticCalculator;
import org.jgrapht.GraphPath;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;

public abstract class NetworkBroadcastProcessFunctionSync1tuple<G> extends KeyedBroadcastProcessFunction<Integer, Tuple2<Integer,Long>, Tuple9<String, Integer, String, Integer, Long, Long, Integer,Integer, Long>, String> {
    final Class<G> typeParameterClass;
    protected ValueState<Integer> currentEdgeIndexVState = null;
    protected ValueState<Double> lastAzimuthVState = null;
    protected ValueState<G> lastGeometryVState = null;
    protected NetworkDistribution networkDistribution;
    protected Map<Integer, GraphPath<String, DefaultWeightedEdge>> shortestIDPathMap = null;
    protected ValueState<Long> seqID = null; // Trajectory points sequence id to help end user generate trajectory from output stream
    protected CoordinateReferenceSystem crs;
    protected double displacementMetersPerSecond;

    protected transient GeodeticCalculator gc;
    protected ValueState<HashSet<Integer>> objIDVState = null;
    protected ValueState<HashSet<Integer>>  removeIDVState = null;

    protected ValueState<Long> TUCounter = null;
    protected ValueState<Long> TScurrBatchID = null;

    protected Random random;

    protected Random timeGen;

    // broadcast state descriptor
    MapStateDescriptor<String, Tuple8<Integer, Long, Integer, Long, Long, Integer, Long, Long>>  edgeTrafficMapDesc;

    MapStateDescriptor<String, Tuple4<Long, Long, Long, Long>> syncState;
    MapStateDescriptor<String,HashSet<Integer>> objIDState;
    MapStateDescriptor<String,HashSet<Integer>> removeIDState;
    MapStateDescriptor<String,HashSet<Integer>> expectedobjIDState;



    public NetworkBroadcastProcessFunctionSync1tuple(Class<G> typeParameterClass, NetworkDistribution networkDistribution,
                                                     Map<Integer, GraphPath<String, DefaultWeightedEdge>> shortestIDPathMap,
                                                     CoordinateReferenceSystem crs, double displacementMetersPerSecond, Random random) {
        this.networkDistribution = networkDistribution;
        this.typeParameterClass = typeParameterClass;
        this.shortestIDPathMap = shortestIDPathMap;
        this.crs = crs;
        this.displacementMetersPerSecond = displacementMetersPerSecond;
        this.random = random;

    }


    @Override
    public void open(Configuration conf) {
        timeGen = new Random();
        this.gc = new GeodeticCalculator(crs);
//        // initialize Map broadcast state
//        edgeTrafficMapDesc = new MapStateDescriptor<>("edgeTrafficMap", TypeInformation.of(new TypeHint<DefaultWeightedEdge>() {}), Types.INT);
        this.edgeTrafficMapDesc = new MapStateDescriptor<>("edgeTrafficMap", BasicTypeInfo.STRING_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Long.class, Long.class, Integer.class, Long.class, Long.class));
        this.syncState = new MapStateDescriptor<>("syncState", BasicTypeInfo.STRING_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Long.class, Long.class, Long.class, Long.class));
//        this.syncState = new MapStateDescriptor<>("syncState", BasicTypeInfo.STRING_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Long.class, Long.class, Long.class, Long.class, TypeInformation.of(new TypeHint<HashSet<Integer>>() {})));

        this.objIDState= new MapStateDescriptor<>(
                "objIDState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));


        this.removeIDState= new MapStateDescriptor<>(
                "removeIDState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));

        this.expectedobjIDState= new MapStateDescriptor<>(
                "expectedobjIDState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));


        ValueStateDescriptor<Integer> currentEdgeIndexVStateDescriptor = new ValueStateDescriptor<>(
                "currentEdgeIndexVStateDescriptor", // the state name
                TypeInformation.of(new TypeHint<Integer>() {}));
        this.currentEdgeIndexVState = getRuntimeContext().getState(currentEdgeIndexVStateDescriptor);

        if(TypeInformation.of(typeParameterClass) instanceof GenericTypeInfo<?>){
            GenericTypeInfo<G> genericTypeInfo = (GenericTypeInfo<G>) TypeInformation.of(typeParameterClass);
            ValueStateDescriptor<G> lastGeometryVStateDescriptor = new ValueStateDescriptor<G>(
                    "lastGeometryVStateDescriptor", // state name
                    genericTypeInfo);
            this.lastGeometryVState = getRuntimeContext().getState(lastGeometryVStateDescriptor);
        }else{
            PojoTypeInfo<G> genericTypeInfo = (PojoTypeInfo<G>) TypeInformation.of(typeParameterClass);
            ValueStateDescriptor<G> lastGeometryVStateDescriptor = new ValueStateDescriptor<G>(
                    "lastGeometryVStateDescriptor", // state name
                    genericTypeInfo);
            this.lastGeometryVState = getRuntimeContext().getState(lastGeometryVStateDescriptor);
        }

        ValueStateDescriptor<Double> lastAzimuthVStateDescriptor = new ValueStateDescriptor<>(
                "lastAzimuthVStateDescriptor", // the state name
                TypeInformation.of(new TypeHint<Double>() {}));
        this.lastAzimuthVState = getRuntimeContext().getState(lastAzimuthVStateDescriptor);


        ValueStateDescriptor<Long> seqIDDescriptor = new ValueStateDescriptor<>(
                "seqID", // the state name
                TypeInformation.of(new TypeHint<Long>() {}),
                0L);
        this.seqID = getRuntimeContext().getState(seqIDDescriptor);


        ValueStateDescriptor<Long> batchIDCountDescriptor= new ValueStateDescriptor<>(
                "batchIDCountDescriptor", // the state name
                TypeInformation.of(new TypeHint<Long>() {}),
                0L);
        this.TUCounter = getRuntimeContext().getState(batchIDCountDescriptor);

        ValueStateDescriptor<Long> TScurrBatchIDDesc = new ValueStateDescriptor<>(
                "TScurrBatchIDDesc", // the state name
                TypeInformation.of(new TypeHint<Long>() {}),
                1L);
        this.TScurrBatchID = getRuntimeContext().getState(TScurrBatchIDDesc);



        ValueStateDescriptor<HashSet<Integer>> objIDVStateDescriptor = new ValueStateDescriptor<>(
                "objIDVStateDescriptor", // the state name
                TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));
        this.objIDVState =  getRuntimeContext().getState(objIDVStateDescriptor);

        ValueStateDescriptor<HashSet<Integer>> removeIDVStateDescriptor = new ValueStateDescriptor<>(
                "removeIDVStateDescriptor", // the state name
                TypeInformation.of(new TypeHint<HashSet<Integer>>() {}));
        this.removeIDVState = getRuntimeContext().getState(removeIDVStateDescriptor);




//
    }
}


