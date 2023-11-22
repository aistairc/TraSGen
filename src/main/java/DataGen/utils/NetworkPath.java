package DataGen.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.interfaces.AStarAdmissibleHeuristic;
import org.jgrapht.alg.interfaces.ShortestPathAlgorithm;
import org.jgrapht.alg.shortestpath.AStarShortestPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NetworkPath implements Serializable {

    private transient ShortestPathAlgorithm<String, DefaultWeightedEdge> shortestPathAlgorithm = null;
    private GeoJSONMapToGraph geoJSONMapToGraph = null;
    private transient Map<Integer, GraphPath<String, DefaultWeightedEdge>> shortestIDTrajsMap = new HashMap<>();
//    private Map<Integer, List<Tuple2<Coordinate, Coordinate>>> shortestIDTrajCoordinatesMap = new HashMap<>();

    public NetworkPath(){}

    public Graph<String, DefaultWeightedEdge> defineNetwork(String mapFile, String mapFileFormat, Double nodeMappingTolerance){

        System.out.println("Defining Network");

        // Converting GeoJSON File into Graph
        geoJSONMapToGraph = new GeoJSONMapToGraph(mapFile, mapFileFormat, nodeMappingTolerance);
        geoJSONMapToGraph.generateGraph();


        Map<String, Coordinate> vertexIDCoordinateMap =  geoJSONMapToGraph.getNodeIDCoordinateMap();
        List<Tuple3<String, String, Map<String, String>>> edgeList = geoJSONMapToGraph.getEdgeList();

        System.out.println("num vertices: " + geoJSONMapToGraph.getNumVertices());
        System.out.println("num edges: " + geoJSONMapToGraph.getNumEdges());

        // Directed Weighted graph
        Graph<String, DefaultWeightedEdge> directedWeightedGraph =  new DefaultDirectedWeightedGraph<>(DefaultWeightedEdge.class);

        // adding nodes
        for (Map.Entry<String, Coordinate> entry : vertexIDCoordinateMap.entrySet()) {
            directedWeightedGraph.addVertex(entry.getKey());
        }

        // adding edges
        for (Tuple3<String, String, Map<String, String>> edge :edgeList) {
            // The two edges must not be equal to avoid loop
            if(!edge.f0.equals(edge.f1)) {
                // Since it is directed graph, both the road sides must be added to the graph
                DefaultWeightedEdge dEdge1 = directedWeightedGraph.addEdge(edge.f0, edge.f1);
                DefaultWeightedEdge dEdge2 = directedWeightedGraph.addEdge(edge.f1, edge.f0);

                // the created edge must be valid
                double edgeWeight = Double.parseDouble(edge.f2.get("weight"));

                if (dEdge1 != null) {
                    directedWeightedGraph.setEdgeWeight(dEdge1, edgeWeight);
                }

                if (dEdge2 != null) {
                    directedWeightedGraph.setEdgeWeight(dEdge2, edgeWeight);
                }
            }
        }
//        BiconnectivityInspector<String, DefaultWeightedEdge> biconnectivityInspector = new BiconnectivityInspector<>(directedWeightedGraph);
//        System.out.println("Bridges " + biconnectivityInspector.getBridges());
//        System.out.println("Cut points " + biconnectivityInspector.getCutpoints());
//        System.out.println("Is Connected " + biconnectivityInspector.isConnected());

        return directedWeightedGraph;
    }

    public ShortestPathAlgorithm<String, DefaultWeightedEdge> defineShortestPathOnGraph(Graph<String, DefaultWeightedEdge> directedWeightedGraph, String shortestPathAlgorithmStr){

        switch (shortestPathAlgorithmStr) {
            case "astar":
            {
                shortestPathAlgorithm = new AStarShortestPath<>(directedWeightedGraph, new AStarAdmissibleHeuristic<String>() {
                    @Override
                    public double getCostEstimate(String sourceVertex, String targetVertex) {
                        // System.out.println(shortestPathAlgorithmStr + ": " + sourceVertex + ", " + targetVertex);
                        //return Math.random()* 100 + 1;
                        return 0;
                    }
                });
                break;
            }
            default:  // dijkstra
                shortestPathAlgorithm = new DijkstraShortestPath<>(directedWeightedGraph);
        }

        return shortestPathAlgorithm;
    }

    public GraphPath<String, DefaultWeightedEdge> getShortestPath(String startingNode, String endingNode){
        return shortestPathAlgorithm.getPath(startingNode, endingNode);
    }

    public List<Tuple2<Coordinate, Coordinate>> getShortestPathCoordinates(String startingNode, String endingNode){

        GraphPath<String, DefaultWeightedEdge> shortestPath = shortestPathAlgorithm.getPath(startingNode, endingNode);
        return shortestPathToCoordinatesList(shortestPath);
    }

    public List<Tuple2<Coordinate, Coordinate>> shortestPathToCoordinatesList(GraphPath<String, DefaultWeightedEdge> shortestPath){

        List<Tuple2<Coordinate, Coordinate>> edgeCoordinatesList = new ArrayList<>();
        List<DefaultWeightedEdge> edgeList = shortestPath.getEdgeList();


        for (DefaultWeightedEdge edge:edgeList) {
            String edgeSource = shortestPath.getGraph().getEdgeSource(edge).toString();
            Coordinate edgeSourceCoordinates = getNodeCoordinate(edgeSource);

            String edgeTarget = shortestPath.getGraph().getEdgeTarget(edge).toString();
            Coordinate edgeTargetCoordinates = getNodeCoordinate(edgeTarget);

            edgeCoordinatesList.add(Tuple2.of(edgeSourceCoordinates, edgeTargetCoordinates));
        }

        return edgeCoordinatesList;
    }

    public double getShortestPathWeight(String startingNode, String endingNode){
        return shortestPathAlgorithm.getPathWeight(startingNode, endingNode);
    }

    public Coordinate getNodeCoordinate(String nodeID){
        return geoJSONMapToGraph.getNodeCoordinate(nodeID);
    }

    public Map<Integer, GraphPath<String, DefaultWeightedEdge>> setInitialShortestTrajs(int minObjID, int maxObjID, String trajStartEndSelectionApproach, List<List<Double>> trajStartEndCoordinatePairs,
                                                                                        List<List<Double>>  trajStartPolygons, List<List<Double>>  trajEndPolygons) {

        int numNodes = geoJSONMapToGraph.getNumVertices();
        int numNodesHalf = numNodes/2; // returns floor(numNodes/2)
        //List<GraphPath<String, DefaultWeightedEdge>> shortestTrajs = new ArrayList<>();
        //Map<Integer, GraphPath<String, DefaultWeightedEdge>> shortestIDTrajsMap = new HashMap<>();

        switch (trajStartEndSelectionApproach) {
            case "random": {
                System.out.println("Generating Randomized Valid Paths...");
                for (int i = minObjID; i <= maxObjID; i++) {
                    // Coordinate c1 = geoJSONMapToGraph.getNodeCoordinate(String.valueOf(HelperClass.getRandomIntInRange(0, numNodesHalf - 1)));
                    // Coordinate c2 = geoJSONMapToGraph.getNodeCoordinate(String.valueOf(HelperClass.getRandomIntInRange(numNodesHalf, numNodes - 1)));

                    GraphPath<String, DefaultWeightedEdge> shortestPath;
                    do {

                        // Threaded (and unseeded) random generation
                        String node1 = String.valueOf(HelperClass.getRandomIntInRange(0, numNodesHalf - 1));
                        String node2 = String.valueOf(HelperClass.getRandomIntInRange(numNodesHalf, numNodes - 1));

                        // Seeded random generation
//                        String node1 = String.valueOf(HelperClass.getRandomIntInRangeWithoutThread(0, numNodesHalf));
//                        String node2 = String.valueOf(HelperClass.getRandomIntInRangeWithoutThread(numNodesHalf, numNodes-1));

                        //System.out.println("node1: " + node1 + ", node2: " + node2);
                        shortestPath = getShortestPath(node1, node2);

                    } while (shortestPath == null);

                    shortestIDTrajsMap.put(i, shortestPath);
                }
                System.out.println("Found All Valid Paths!");
                break;
            }

            case "userdefined": {

                TupleItemDistance itemDistance = new TupleItemDistance();
                GraphPath<String, DefaultWeightedEdge> shortestPath;
                int i = minObjID;

                Polygon bufferedConvexHull = geoJSONMapToGraph.getBufferedConvexHull();
                STRtree nodesSTRtree = geoJSONMapToGraph.getNodesSTRtree();


                for (List<Double> list :  trajStartEndCoordinatePairs) {
                    //Tuple2<Coordinate, Coordinate> startEndCoordinates = Tuple2.of(new Coordinate(list.get(0), list.get(1)), new Coordinate(list.get(3), list.get(4)));

                    Coordinate startCoordinate = new Coordinate(list.get(0), list.get(1));
                    Coordinate endCoordinate = new Coordinate(list.get(3), list.get(4));
                    Point startPoint = new GeometryFactory().createPoint(startCoordinate);
                    Point endPoint = new GeometryFactory().createPoint(endCoordinate);

                    if (bufferedConvexHull.contains(startPoint) & bufferedConvexHull.contains(endPoint)) {

                            Tuple2<Point,String> startNodePoint = (Tuple2<Point,String>) nodesSTRtree.nearestNeighbour(startPoint.getEnvelopeInternal(), Tuple2.of(startPoint,null), itemDistance);
                            String node1 = startNodePoint.f1;
                            Tuple2<Point,String> endNodePoint = (Tuple2<Point,String>) nodesSTRtree.nearestNeighbour(endPoint.getEnvelopeInternal(), Tuple2.of(endPoint,null), itemDistance);
                            String node2 = endNodePoint.f1;
                            shortestPath = getShortestPath(node1, node2);

                            if (shortestPath == null) throw new NullPointerException("Shortest path does not exist between " + startCoordinate.toString() + " and " + endCoordinate.toString());

                            shortestIDTrajsMap.put(i, shortestPath);
                            i++;
                    }
                    else throw new IllegalArgumentException("Start Point " + startCoordinate.toString() + " or " + endCoordinate.toString() + " do not exist on convex hull");
                }
                break;
            }

            case "region": {

                GeometryFactory gf = new GeometryFactory();

                MultiPolygon startMultiPolygon = generatePolygonList(trajStartPolygons, gf);
                MultiPolygon endMultiPolygon = generatePolygonList(trajEndPolygons, gf);

                System.out.println("Generating Valid Paths...");

                for (int i = minObjID; i <= maxObjID; i++) {
                    GraphPath<String, DefaultWeightedEdge> shortestPath = null;
                    int count = 0;

                    do {
                        // Threaded (and unseeded) random generation
                        String node1 = String.valueOf(HelperClass.getRandomIntInRange(0, numNodesHalf - 1));
                        String node2 = String.valueOf(HelperClass.getRandomIntInRange(numNodesHalf, numNodes - 1));

                        // Seeded random generation
//                        String node1 = String.valueOf(HelperClass.getRandomIntInRangeWithoutThread(0, numNodesHalf));
//                        String node2 = String.valueOf(HelperClass.getRandomIntInRangeWithoutThread(numNodesHalf, numNodes-1));

                        Point startPoint = new GeometryFactory().createPoint(geoJSONMapToGraph.getNodeCoordinate(node1));
                        Point endPoint = new GeometryFactory().createPoint(geoJSONMapToGraph.getNodeCoordinate(node2));

                        // check if randomly selected points fall inside the polygons
                        if (startMultiPolygon.contains(startPoint) & endMultiPolygon.contains(endPoint)) {
                            shortestPath = getShortestPath(node1, node2);
                        }
                        count++;
                        // break if cannot find shortest path after 10000 attempts
                        if (count > 10000) throw new NullPointerException("Shortest path does not exist between start polygons and end polygons" );

                    } while (shortestPath == null);

                    shortestIDTrajsMap.put(i, shortestPath);
            }
                System.out.println("Found All Valid Paths!");
                break;
        }

        default: {  // default
                shortestIDTrajsMap = null;
            }
        }
        return shortestIDTrajsMap;
    }

    public MultiPolygon generatePolygonList (List<List<Double>> doubleCoordinateList, GeometryFactory gf) {

        Polygon[] polygonList = new Polygon[doubleCoordinateList.size()];

        // iterate through polygon list
        for (int k = 0 ; k < doubleCoordinateList.size(); k++) {
            List<Double> list = doubleCoordinateList.get(k);
            int i = 0;
            Coordinate [] polygonCoordinates = new Coordinate[list.size()/3];
            //iterate through coordinate list of each polygon
            for (int j = 0 ; j < list.size() ; j=j+3)
            {
                Coordinate c = new Coordinate(list.get(j),list.get(j+1), list.get(j+2));
                polygonCoordinates[i] = c;
                i++;
            }
            // check for percentage overlap between convexhull and aredefined polygon
            Polygon polygon = gf.createPolygon(polygonCoordinates);
            Polygon bufConvexHull = geoJSONMapToGraph.getBufferedConvexHull();
            Geometry intersect = bufConvexHull.intersection(polygon);
            double percOverlap = 100* intersect.getArea()/(polygon.getArea() < bufConvexHull.getArea() ? polygon.getArea() : bufConvexHull.getArea());
            if (percOverlap < 10) throw new IllegalArgumentException(polygon.toText() + " does not have sufficient overlap with road region (convexHull)");
            polygonList[k] = polygon;
        }

        MultiPolygon multiPolygon = gf.createMultiPolygon(polygonList);

        return multiPolygon;

    }

}
