package DataGen.geometryGenerator;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;

public class MultiPolygonGenerator extends MultiGenerator {

    public MultiPolygonGenerator(PolygonGenerator generator) {
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

            default:
                throw new IllegalStateException("Invalid Alg. Specified");
        }

        while(grid.canCreate()){
            generator.setBoundingBox(grid.createEnv());
            geoms.add(generator.create());
        }

        return geometryFactory.createMultiPolygon((Polygon[]) geoms.toArray(new Polygon[numberGeometries]));
    }
}
