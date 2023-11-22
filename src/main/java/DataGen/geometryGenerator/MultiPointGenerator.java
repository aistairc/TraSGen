package DataGen.geometryGenerator;

import DataGen.utils.HelperClass;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;

public class MultiPointGenerator extends MultiGenerator {

    public MultiPointGenerator(PointGenerator generator) {
        super(generator);
    }

    /**
     * Creates a geometry collection representing the set of child geometries created.
     *
     * @see #setNumberGeometries(int)
     * @see org.locationtech.jts.generator.GeometryGenerator#create()
     *
     * @see #BOX
     * @see #VERT
     * @see #HORZ
     * @see #RND
     *
     * @throws NullPointerException when the generator is missing
     * @throws IllegalStateException when the number of child geoms is too small
     * @throws IllegalStateException when the selected alg. is invalid
     */
    public Geometry create() {
        if(generator == null)
            throw new NullPointerException("Missing child generator");

        if(numberGeometries < 1)
            throw new IllegalStateException("Too few child geoms to create");

        ArrayList<Geometry> geoms = new ArrayList(numberGeometries);

        GridGenerator grid = GeometryGenerator.createGridGenerator();
        grid.setBoundingBox(boundingBox);
        grid.setGeometryFactory(geometryFactory);

        switch(generationAlgorithm){
            case BOX:

                int nrow = (int)Math.sqrt(numberGeometries);
                int ncol = numberGeometries/nrow;
                grid.setNumberRows(nrow);
                grid.setNumberColumns(ncol);

                break;
            case VERT:

                grid.setNumberRows(1);
                grid.setNumberColumns(numberGeometries);

                break;
            case HORZ:

                grid.setNumberRows(numberGeometries);
                grid.setNumberColumns(1);

                break;

            case RND:
                for(int i = 0; i < numberGeometries; i++) {
                    double x = HelperClass.getRandomDoubleInRange(boundingBox.getMinX(), boundingBox.getMaxX());
                    double y = HelperClass.getRandomDoubleInRange(boundingBox.getMinY(), boundingBox.getMaxY());

                    Coordinate c = new Coordinate(x, y);
                    geoms.add(geometryFactory.createPoint(c));
                }
                grid.setNumberRows(0);
                grid.setNumberColumns(0);
                break;

            default:
                throw new IllegalStateException("Invalid Alg. Specified");
        }

        while(grid.canCreate()){
            generator.setBoundingBox(grid.createEnv());
            geoms.add(generator.create());
        }

        return geometryFactory.createMultiPoint((Point[]) geoms.toArray(new Point[numberGeometries]));
    }
}
