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

package DataGen;

import DataGen.inputParameters.Params;
import DataGen.timeSeriesGenerators.*;
import DataGen.timeSeriesGenerators.network.NetworkDistribution;
import DataGen.timeSeriesGenerators.network.NetworkWalk;
import DataGen.utils.IntegerStream;
import DataGen.utils.IntegerStreamBroadcast;
import DataGen.utils.Serialization;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob implements Serializable {

	public static void main(String[] args) throws Exception {

		// Required for populating the variables
		Params params = new Params();
		System.out.println(params.toString());

		boolean onCluster = Params.clusterMode;
		int parallelism = Params.parallelism;
		String trajectoryType = Params.trajectoryType;
		String datatypeOption = Params.datatypeOption;
		String outputOption = Params.outputOption;
		String outputTopicName = Params.outputTopicName;
		String bootStrapServers = Params.bootStrapServers;

		String redisAddresses = Params.redisAddresses;
		String redisServerType = Params.redisServerType;


		String format = Params.outputFormat;
		long numRows = Params.nRows;

		String trajStartEndSelectionApproach = Params.trajStartEndSelectionApproach;
		List<List<Double>> trajStartEndCoordinatePairs = Params.trajStartEndCoordinatePairs;

		List<List<Double>> trajStartPolygons = Params.trajStartPolygons;
		List<List<Double>> trajEndPolygons= Params.trajEndPolygons;

		int minObjID = Params.objIDRange.get(0);
		int maxObjID = Params.objIDRange.get(1);

		if (trajStartEndSelectionApproach.equals("userdefined")) {

			minObjID = 1;
			maxObjID = trajStartEndCoordinatePairs.size();

		}

		// if-else condition

		String dateTimeFormat = Params.dateFormat;
		String initialTimeStamp = Params.initialTimeStamp;
		int timeStep = 	Params.timeStep;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateTimeFormat);

		String mapFile = Params.mapFile;
		String mapFileFormat = Params.mapFileFormat;
		String shortestPathAlgorithmStr = Params.shortestPathAlgorithm;
		Double nodeMappingTolerance = Params.nodeMappingTolerance;
		String interWorkersDataSharing = Params.interWorkersDataSharing;
		boolean sync = Params.sync;
		boolean randomizeTimeInBatch = Params.randomizeTimeInBatch;

		CoordinateReferenceSystem crs = Params.coordinateReferenceSystem;
		Double displacementMetersPerSecond = Params.displacementMetersPerSecond;

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setString(RestOptions.BIND_PORT, "8082");

		// Preparing Kafka Connection
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");

		// Set up the Flink streaming execution environment
		final StreamExecutionEnvironment env;
		if (onCluster) {
			env = StreamExecutionEnvironment.getExecutionEnvironment();
		}else{
			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		}
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(parallelism);


		// start time
		long startTime = System.nanoTime();

		IntegerStream integerStream = new IntegerStream(env, minObjID, maxObjID, numRows);

		DataStream<Tuple2<Integer,Long>> objIDStreamWithBatchID = null;
		if (!(interWorkersDataSharing.equalsIgnoreCase("broadcast") && sync)) {
			objIDStreamWithBatchID = integerStream.generateRoundRobinWithBatchID();
		}
		else if(interWorkersDataSharing.equalsIgnoreCase("broadcast") && sync) {
			IntegerStreamBroadcast integerStreamBroadcast = new IntegerStreamBroadcast(env, minObjID, maxObjID, numRows, kafkaProperties);
			objIDStreamWithBatchID = integerStreamBroadcast.generateRoundRobin();

		}

		//networkOption
		NetworkDistribution network = null;
		switch (trajectoryType) {
			case "vehicle": {
				network = new NetworkWalk();
				break;
			}
			case "pedestrian": {
				network = new NetworkWalk();
				break;
			}
			default: {
				System.out.println("Unrecognized network option. Please input the appropriate network option.");
			}
		}

		StreamGenerator streamGenerator = null;
		Random timeGen = new Random();
		switch (datatypeOption) {
			case "networkPoint": {

				if(interWorkersDataSharing.equalsIgnoreCase("broadcast")) {
					// with broadcasting traffic congestion
					if (sync) {
						System.out.println("Broadcast interWorkersDataSharing option. Generating data in SYNC mode.");
						streamGenerator = new NetworkPointStreamGeneratorSync1tuple(network, kafkaProperties, env, format, mapFile, mapFileFormat, shortestPathAlgorithmStr,
								nodeMappingTolerance, minObjID, maxObjID, trajStartEndSelectionApproach, trajStartEndCoordinatePairs ,trajStartPolygons, trajEndPolygons, displacementMetersPerSecond, crs, parallelism, Params.syncPercentage,
								dateTimeFormat, initialTimeStamp, timeStep, randomizeTimeInBatch);
					}
					else {
						System.out.println("Broadcast interWorkersDataSharing option. Generating data in ASYNC mode.");
						streamGenerator = new NetworkPointStreamGeneratorAsync(network, kafkaProperties, env, format, mapFile, mapFileFormat, shortestPathAlgorithmStr,
								nodeMappingTolerance, minObjID, maxObjID, trajStartEndSelectionApproach, trajStartEndCoordinatePairs ,trajStartPolygons, trajEndPolygons, displacementMetersPerSecond, crs,
								dateTimeFormat, initialTimeStamp, timeStep, randomizeTimeInBatch);
					}

				} else {
					if(Params.interWorkersDataSharing.equalsIgnoreCase("redis")) {System.out.println("Redis interWorkersDataSharing option. Data sharing will be done using REDIS.");}
					streamGenerator = new NetworkPointStreamGenerator(network, format, mapFile, mapFileFormat, shortestPathAlgorithmStr, nodeMappingTolerance,
							minObjID, maxObjID, trajStartEndSelectionApproach, trajStartEndCoordinatePairs,	trajStartPolygons, trajEndPolygons, displacementMetersPerSecond, crs, interWorkersDataSharing, redisAddresses, redisServerType,
							dateTimeFormat, initialTimeStamp, timeStep, randomizeTimeInBatch);
				}
				break;
			}
			default:
				System.out.println("Unrecognized datatype option. Please input the appropriate datatype option.");
		}

		assert (streamGenerator != null) : "streamGenerator is null";

		DataStream<String> geometryStream;
		geometryStream = streamGenerator.generate(objIDStreamWithBatchID, simpleDateFormat);

		switch (outputOption) {
			case "kafka": {
				//Sending output stream to Kafka
				geometryStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new Serialization.StringToStringOutput(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).disableChaining().name("Trajectory Points");
				break;
			}
			case "file":{
				DefaultRollingPolicy.PolicyBuilder policyBuilder = DefaultRollingPolicy.create();
				if(Params.outputRollingRolloverInterval > 0L)
					policyBuilder = policyBuilder.withRolloverInterval(Params.outputRollingRolloverInterval);
				if(Params.outputRollingInactivityInterval > 0L)
					policyBuilder = policyBuilder.withInactivityInterval(Params.outputRollingInactivityInterval);
				if(Params.outputRollingMaxPartSize > 0L)
					policyBuilder = policyBuilder.withMaxPartSize(Params.outputRollingMaxPartSize);

				final StreamingFileSink<String> sink = StreamingFileSink
						.forRowFormat(new Path(Params.outputDirName), new SimpleStringEncoder<String>("UTF-8"))
						.withRollingPolicy(policyBuilder.build())
						.build();

				geometryStream.addSink(sink).name("Trajectory Points");
				break;
			}
			default:
				System.out.println("Unrecognized output option.");
		}

		// execute program
		env.execute("TraSGen");

		long endTime = System.nanoTime();
		// get difference of two nanoTime values
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in milliseconds : " + timeElapsed / 1000000);
	}
}