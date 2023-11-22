package DataGen.timeSeriesGenerators;

import DataGen.inputParameters.Params;
import DataGen.timeSeriesGenerators.random.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractRichMapFunction<G> extends RichMapFunction<Tuple2<Integer,Long>, String> {

    final Class<G> typeParameterClass;
    protected RandomDistribution randomDistribution;
    protected RandomWalk randomWalk;
    protected HotspotDistribution hotspotDistribution;
    protected HotspotGaussianRandomWalk hotspotGaussianRandomWalk;

    protected  Envelope seriesBBox;

    protected Random timeGen;

    protected double seriesVar;
    protected String randomOption;
    protected List<List<Double>> hotspotMean;
    protected List<List<Double>> hotspotBBox;
    protected List<List<Double>> hotspotVariance;

    protected ValueState<G> lastGeometryVState = null;
    protected ValueState<LocalDateTime> lastTimestampVState = null;


    public AbstractRichMapFunction(Class<G> typeParameterClass, Envelope seriesBBox,  String randomOption, double seriesVar,
                                   List<List<Double>> hotspotVariance, List<List<Double>> hotspotMean, List<List<Double>> hotspotBBox) {
        this.typeParameterClass = typeParameterClass;
        this.seriesBBox = seriesBBox;
        this.randomOption = randomOption;
        this.seriesVar = seriesVar;
        this.hotspotVariance = hotspotVariance;
        this.hotspotMean = hotspotMean;
        this.hotspotBBox = hotspotBBox;
    }


    @Override
    public void open(Configuration config) {
        timeGen = new Random();

        List<Coordinate> listHotspotMean = new ArrayList<>();
        List<Coordinate> listHotspotVariance = new ArrayList<>();
        List<Tuple2<Coordinate, Coordinate>> listHotspotBBox = new ArrayList<>();

        switch (randomOption) {
            case "hotspot":
            case "hotspotRW": {
                for (List<Double> list : hotspotMean) {
                    listHotspotMean.add(new Coordinate(list.get(0), list.get(1)));
                }
                for (List<Double> list : hotspotVariance) {
                    listHotspotVariance.add(new Coordinate(list.get(0), list.get(1)));
                }
                for (List<Double> list : hotspotBBox) {
                    listHotspotBBox.add( Tuple2.of(new Coordinate(list.get(0), list.get(1)), new Coordinate(list.get(3), list.get(4))));
                }
                break;
            }
        }

        switch (randomOption) {
            case "uniform": {
                randomDistribution = new UniformDistribution();
                break;
            }
            case "gaussian" : {
                randomDistribution = new GaussianDistribution();
                break;
            }
            case "gaussianRW":
            {
                randomDistribution = new GaussianRandomWalk(seriesVar);
                break;
            }
            case "brownRW" : {
                randomDistribution = new BrownianMotionRandomWalk(seriesVar);
                break;
            }
            case "hotspot" :
            {
                randomDistribution = new HotspotGaussianDistribution(listHotspotMean, listHotspotVariance);
                break;
            }
            case "hotspotRW" :
            {
                randomDistribution = new HotspotGaussianRandomWalk(listHotspotMean, listHotspotVariance, listHotspotBBox, seriesVar);
                break;
            }
            default:
                System.out.println("Unrecognized random option. Please input the appropriate random option.");
        }



        if (randomDistribution instanceof RandomWalk) {
            randomWalk = (RandomWalk) randomDistribution;

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

            GenericTypeInfo<LocalDateTime> localDateTimeTypeInfo = (GenericTypeInfo<LocalDateTime>) TypeInformation.of(LocalDateTime.class);
            ValueStateDescriptor<LocalDateTime> lastTimestampVStateDescriptor = new ValueStateDescriptor<>(
                    "lastTimestampVStateDescriptor", // state name
                    localDateTimeTypeInfo);
            this.lastTimestampVState = getRuntimeContext().getState(lastTimestampVStateDescriptor);

            if (randomDistribution instanceof HotspotGaussianRandomWalk) {
                hotspotGaussianRandomWalk = (HotspotGaussianRandomWalk) randomDistribution;
            }
        }
        else if (randomDistribution instanceof HotspotDistribution){
            hotspotDistribution = (HotspotDistribution) randomDistribution;

        }
    }


}