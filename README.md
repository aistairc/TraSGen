# TraSGen: A Distributed, Scalable and Continuous Trajectory Stream Generator
A brief description of essential requirements for operating TraSGen are given below:

## External Dependencies

TraSGen accepts any road network directly in GeoJSON format provided the geometry types are "LineString" or "MultiLineString". The geometries can have additional properties, for instance max road segment speed, number of lanes, road type, etc. which can be used by moving object speed model to compute a moving object's next location on the network. TraSGen converts GeoJSON file into directed weighted graph using JGraphT library, which is a Java library of graph data structures and algorithms. TraSGen converts GeoJSON file to Graph only once in the beginning when the file is read, thus it does not affect the overall TraSGen performance.

The GeoJSON network of any area, city, or country is available for download for free from OpenStreetMap. Geojson.io provides an interactive tool to build custom GeoJSON network. Similarly, GeoJSONMaps provides a web API to build and download GeoJSON maps.

# Configuration Parameters

To run TrasGen, the main class called "DataGen.StreamingJob" needs to be executed where upon a flink job called "TrasGen" will be instantiated that starts trajectory generation. TrasGen can be controlled by setting the parameters in the "spatialdatagen-conf.yml" file which are then read by the main class "DataGen.StreamingJob". The default values of the parameters are set in the mentioned file. A brief description for these parameters is given below.

## Cluster
Properties for Apache Flink cluster

| Name        | clusterMode                                                                       |
|-------------|-----------------------------------------------------------|
| Value       | {True, False}                                                                     |
| Description | Set True to run TraSGen on a multi node Flink cluster. False for standalone mode. |


| Name        | parallelism                                                                                                          |
|-------------|-----------------------------------------------------------|
| Value  | 1\~number of parallel instances                                                                                      |
| Description | The number of parallel instances in clusterMode is equal to the number of Task Slots available in the Flink cluster. |

## Output Parameters (output)
Output sink type and format for the generated trajectories.

| Name        | option                                                                                                                                                      |
|-------------|-----------------------------------------------------------|
| Value       | {"kafka", "file"}                                                                                                                                           |
| Description | The type of output sink for the output of trajectory points. Select option 'kafka' for an Apache Kafka sink or file to save the data locally in a txt file. |

| Name        | outputformat                                 |
|-------------|----------------------------------------------|
| Value       | {"GeoJSON", "WKT"}                           |
| Description | The output format for the trajectory points. |

-   **kafka:** Define the properties of the kafka output sink.

| Name        | outputTopicName                                           |
|-------------|-----------------------------------------------------------|
| Value       | String                                                    |
| Description | The name of the kafka topic designated as the output sink |

| Name        | bootStrapServers                                                                                                                                                                                                                       |
|------------|------------------------------------------------------------|
| Value       | IP address                                                                                                                                                                                                                             |
| Description | If output "option" is selected as "kafka" then provide the IP address and port of kafka server here. For multi-node kafka server the IP addresses can be defined in comma delimited format. E.g \"172.16.0.64:9092, 172.16.0.81:9092\" |

-   **file:** Specify the path of the output file.

| Name        | outputDirName                                                                    |
|------------|------------------------------------------------------------|
| Value       | File path                                                                        |
| Description | If output "option" is selected as "file" then provide the path of the file here. |

-   **data:** Properties of the output trajectory stream.

| Name        | dateFormat                                                                                                          |
|------------|---------------------------------------------------------------------------------------------------------------------|
| Value       | \"yyyy-MM-dd HH:mm:ss\" and variations, "unix"                                                                      |
| Description | Output format for the timestamp of each trajectory data point. If "unix",timeStamps will be in Epoch/UNIX GMT time. |

| Name        | initialTimeStamp                                                                                                         |
|------------|--------------------------------------------------------------------------------------------------------------------------|
| Value       | Any timestamp provided in the format defined by "dateFormat", <br/>"system"                                              |
| Description | Starting timestamp of the first point in the trajectories. If "system" then the current time is used for all timestamps. |

| Name        | timeStep                                                                                                                                                                                                                                                                                                 |
|------------|------------------------------------------------------------|
| Value       | Integer                                                                                                                                                                                                                                                                                                  |
| Description | A discrete increment in milliseconds for each timestamp of subsequent points in a trajectory. Please note that this not the actual wait time between the generation of points but rather just an incremental value for the labelling of timeSteps for each trajectory point after the initial time step. |

| Name        | randomizeTimeInBatch                                                                                                                                                                    |
|------------|------------------------------------------------------------|
| Value       | {True, False}                                                                                                                                                                           |
| Description | Set False to set the same timestamp for all parallel generated trajectory points. Set True for all parallel points generated in parallel to have a different timestamp from each other. |

| Name        | objIDRange                                                      |
|------------|------------------------------------------------------------|
| Value       | \[1, N+1\]                                                      |
| Description | Total number of N trajectories to be generated by the generator |

| Name        | nRows                                                                                                                                                                             |
|------------|------------------------------------------------------------|
| Value       | Integer                                                                                                                                                                           |
| Description | The combined total number of output points to be generated by all trajectories. Set to "-1" to generate unlimited number of points until all trajectories reach their end points. |

| Name        | consecutiveTrajTuplesIntervalMilliSec                                                                                                                                                   |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Value       | Integer                                                                                                                                                                                 |
| Description | The rate of data generation. Set 0 for max throughput. Any value greater than 0 will be the real-time delay in milliseconds in between generation of consecutive points in a trajectory |

## Query Parameters (query)
Control options for trajectory generation
-   **mappedTrajectories:** Control parameters for trajectory generation on network

| Name        | mapFile                                                                                                                                    |
|------------|------------------------------------------------------------|
| Value       | File path                                                                                                                                  |
| Description | Absolute file path for the GeoJSON road map file generate trajectory points. Supported trajectories are "LineString" and "MultiLineString" |

| Name        | shortestPathAlgorithm                                                          |
|------------|------------------------------------------------------------|
| Value       | { "dijkstra", "astar"}                                                         |
| Description | The shortest path algorithm to be used to define the route of the trajectories |

| Name        | interWorkersDataSharing                                                                 |
|------------|------------------------------------------------------------|
| Value       | {"none", "redis", "broadcast"}                                                          |
| Description | The traffic congestion information exchange methodology to be used between the workers. |

| Name        | sync                                                                                                                                                                                                                                                                                                                                                              |
|------------|------------------------------------------------------------|
| Value       | {True, False}                                                                                                                                                                                                                                                                                                                                                     |
| Description | If "interWorkersDataSharing" is set to be "broadcast then set "sync" to true to ensure the latest trajectory point is generated using the latest traffic information i.e waiting until the the road traffic update tables are fully updated. Set false to generate trajectory points without waiting for the road traffic update tables to complete their update. |

| Name        | syncPercentage                                                                                                                                                                                                                                                                                                                                                                                                                  |
|------------|------------------------------------------------------------|
| Value       | Double value between 0.0\~100.0                                                                                                                                                                                                                                                                                                                                                                                                 |
| Description | If "interWorkersDataSharing" is set to be "broadcast and "sync" to true then select how much road traffic information should be shared among workers. 100.0 to wait until the table is updated using all of the traffic tuples generated by all trajectories. 0.0 to update the traffic table using the least amount of traffic tuples (one). The higher the "syncPercentage" the lower the trajectory generation rate or time. |

| Name        | trajStartEndSelectionApproach                                                                                                                                                                                                                                                                                                      |
|------------|------------------------------------------------------------|
| Value       | {"random", "userdefined", "region"}                                                                                                                                                                                                                                                                                                |
| Description | Define how to select start and end points of trajectories in a network. "random" uses a random generator to randomly select the start-end points from the entire road network. "userdefined" uses the start-end pairs provided by the user. "region" randomly selects start-end points from an area or areas selected by the user. |

<table>
<colgroup>
<col style="width: 15%" />
<col style="width: 84%" />
</colgroup>
<thead>
<tr class="header">
<th>Name</th>
<th>trajStartEndCoordinatePairs</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>Value</td>
<td><p>{[Pair A], [Pair B], …[Pair N]}</p>
<p>where [Pair n] is defined as</p>
<p>[start_longitude_n, start_latitude_n, 0.0, end_ longitude_n, end_
latitude_n, 0.0]</p></td>
</tr>
<tr class="even">
<td>Description</td>
<td>Define the start-end coordinate pairs manually selected by the user
if “trajStartEndSelectionApproach” is set to “userdefined”. If the
coordinates do not lie on the road network then the nearest point on the
road network is selected.</td>
</tr>
</tbody>
</table>

<table>
<colgroup>
<col style="width: 15%" />
<col style="width: 84%" />
</colgroup>
<thead>
<tr class="header">
<th>Name</th>
<th>trajStartPolygons</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>Value</td>
<td><p>{[Polygon A], [Polygon B] …[Polygon N]}</p>
<p>where coordinate points in [Polygon N] are defined as</p>
<p>[point1_longitude, point1_latitude, 0.0, point2_ longitude, point2_
latitude, 0.0…. pointX_longitude, pointX_latitude, 0.0]</p></td>
</tr>
<tr class="even">
<td>Description</td>
<td>If “trajStartEndSelectionApproach” is set to “region”. Define a set
of region polygons in which the randomly selected starting points of the
trajectories should lie within.</td>
</tr>
</tbody>
</table>

<table>
<colgroup>
<col style="width: 15%" />
<col style="width: 84%" />
</colgroup>
<thead>
<tr class="header">
<th>Name</th>
<th>trajEndPolygons</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>Value</td>
<td><p>{[Polygon A], [Polygon B] …[Polygon N]}</p>
<p>where coordinate points in [Polygon N] are defined as</p>
<p>[point1_longitude, point1_latitude, 0.0, point2_ longitude, point2_
latitude, 0.0…. pointX_longitude, pointX_latitude, 0.0]</p></td>
</tr>
<tr class="even">
<td>Description</td>
<td>If “trajStartEndSelectionApproach” is set to “region”. Define the
set of polygons in which the randomly selected ending points of the
trajectories should lie within.</td>
</tr>
</tbody>
</table>

| Name        | displacementMetersPerSecond                                 |
|------------|------------------------------------------------------------|
| Value       | Double \>= 1                                                |
| Description | The default speed in meters per second of the trajectories. |

## Redis Parameters (redis)

Define properties for Redis cluster. Set these values if
"interWorkersDataSharing" is selected as "redis".

| Name        | redisServerType                                                                                                       |
|------------|------------------------------------------------------------|
| Value       | {"standalone", "cluster"}                                                                                             |
| Description | Set "cluster" if you have a redis custer available otherwise use "standalone" if you have a local redis installation. |

<table>
<colgroup>
<col style="width: 15%" />
<col style="width: 84%" />
</colgroup>
<thead>
<tr class="header">
<th>Name</th>
<th>redisAddresses</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>Value</td>
<td>IP address</td>
</tr>
<tr class="even">
<td>Description</td>
<td><p>Provide the IP addresses of redis server in comma delimited
format. For example a redis server composed of three nodes:</p>
<p>"redis://172.16.0.126:6379, redis://172.16.0.70:6379,
redis://172.16.0.121:6379"</p></td>
</tr>
</tbody>
</table>


## Valid Geometries (For Synthetic Trajecotry Generation)

| Geometry | Number of Points/Vertices | Number of Holes | Number of Geometries (for Multi Geometries only) | Geometry/MultiGeometry Generation Algorithm |
| ------ | ------ |------ | ------ |------ |
| Point | NA | NA | NA | NA |
| LineString | 3 ~ 360 | NA | NA | 0 (Arc) |
| LineString | 3 ~ 1000 | NA | NA | 1 (Vertical) |
| LineString | 3 ~ 1000 | NA | NA | 2 (Horizontal) |
| Polygon | 4 ~ 1000 | 0 ~ 4 | NA | 0 (Box) |
| Polygon | 4 ~ 20 | 0 | NA | 1 (Arc) |
| Polygon | 5 ~ 20 | 2 ~ 4 | NA | 1 (Arc) |
| MultiPoint | 3 ~ 1000 | NA | 2,3,4,6,8,10 | 0 (Box) |
| MultiPoint | 3 ~ 1000 | NA | 2 ~ 1000 | 1 (Horizontal) |
| MultiPoint | 3 ~ 1000 | NA | 2 ~ 1000 | 2 (Vertical) |
| MultiLineString | 3 ~ 1000 | NA | 2,3,4,6,8,10 | 0 (Box) |
| MultiLineString | 3 ~ 1000 | NA | 2 ~ 1000 | 1 (Horizontal) |
| MultiLineString | 3 ~ 1000 | NA | 2 ~ 1000 | 2 (Vertical) |
| MultiPolygon | 4 ~ 1000 | 0 ~ 4 | 2,3,4,6,8,10 | 0 (Box) |
| MultiPolygon | 4 ~ 20 | 0 | 2 ~ 1000 | 1 (Horizontal) |
| MultiPolygon | 4 ~ 20 | 2 ~ 4 | 2 ~ 1000 | 2 (Vertical) |


