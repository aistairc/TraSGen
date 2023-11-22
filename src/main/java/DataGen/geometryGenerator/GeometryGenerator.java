package DataGen.geometryGenerator;
/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
//package org.locationtech.jts.generator;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * This class illustrates the basic functionality and configuration options for generating spatial data. 
 *
 * @author David Zwiers, Vivid Solutions. 
 */
public abstract class GeometryGenerator {
	protected int dimensions = 2;
	protected GeometryFactory geometryFactory; // includes srid
	protected Envelope boundingBox;
	
	/**
	 * @return A Geometry which uses some or all of the Bounding Box specified.
	 */
	public abstract Geometry create();

	
	/**
	 * @see GridGenerator
	 * @return A new GridGenerator
	 */
	public static GridGenerator createGridGenerator(){
		return new GridGenerator();
	}
	
	/**
	 * @see PointGenerator
	 * @return A new PointGenerator
	 */
	public static PointGenerator createPointGenerator(){
		return new PointGenerator();
	}

	/**
	 * @see LineStringGenerator
	 * @return A new LineStringGenerator
	 */
	public static LineStringGenerator createLineStringGenerator(){
		LineStringGenerator lsg = new LineStringGenerator();
		lsg.setGenerationAlgorithm(LineStringGenerator.ARC);
		lsg.setNumberPoints(10);
		return lsg;
	}

	/**
	 * @see LineStringGenerator
	 * @return A new LineStringGenerator
	 */
	public static LineStringGenerator createLineStringGenerator(int numberPoints, int generationAlgorithm){
		LineStringGenerator lsg = new LineStringGenerator();
		lsg.setGenerationAlgorithm(generationAlgorithm);
		lsg.setNumberPoints(numberPoints);
		return lsg;
	}


	/**
	 * @see PolygonGenerator
	 * @return A new PolygonGenerator
	 */
	public static PolygonGenerator createPolygonGenerator(){
		PolygonGenerator pg = new PolygonGenerator();
		pg.setGenerationAlgorithm(PolygonGenerator.ARC);
		pg.setNumberPoints(10);
		pg.setNumberHoles(8);
		return pg;
	}

	/**
	 * @see PolygonGenerator
	 * @return A new PolygonGenerator
	 */
	public static PolygonGenerator createPolygonGenerator(int numberPoints, int numberHoles, int generationAlgorithm){
		PolygonGenerator pg = new PolygonGenerator();
		pg.setGenerationAlgorithm(generationAlgorithm);
		pg.setNumberPoints(numberPoints);
		pg.setNumberHoles(numberHoles);
		return pg;
	}

	/**
	 * @see PointGenerator
	 * @see MultiGenerator
	 * @return A new MultiGenerator
	 */
	public static MultiGenerator createMultiPointGenerator(){
		MultiGenerator mg = new MultiPointGenerator(createPointGenerator());
		mg.setNumberGeometries(4);
		return mg;
	}

	/**
	 * @see LineStringGenerator
	 * @see MultiGenerator
	 * @return A new PointGenerator
	 */
	public static MultiGenerator createMultiLineStringGenerator(int numberPoints, int generationAlgorithm){

		//MultiGenerator mg = new MultiGenerator(createLineStringGenerator());
		MultiGenerator mg = new MultiLineStringGenerator(createLineStringGenerator(numberPoints, generationAlgorithm));
		mg.setNumberGeometries(4);
		return mg;
	}

	/**
	 * @see PolygonGenerator
	 * @see MultiGenerator
	 * @return A new PointGenerator
	 */
	public static MultiGenerator createMultiPolygonGenerator(int numberPoints, int numberHoles, int generationAlgorithm){
		//MultiGenerator mg = new MultiGenerator(createPolygonGenerator());
		MultiGenerator mg = new MultiPolygonGenerator(createPolygonGenerator(numberPoints, numberHoles, generationAlgorithm));
		mg.setNumberGeometries(4);
		return mg;
	}

	/**
	 * @return Returns the boundingBox.
	 */
	public Envelope getBoundingBox() {
		return boundingBox;
	}

	/**
	 * @param boundingBox The boundingBox to set.
	 */
	public void setBoundingBox(Envelope boundingBox) {
		this.boundingBox = boundingBox;
	}

	/**
	 * @return Returns the dimensions.
	 */
	public int getDimensions() {
		return dimensions;
	}

	/**
	 * @param dimensions The dimensions to set.
	 */
	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
		throw new RuntimeException("Dimensions other than 2 are not yet supported");
	}

	/**
	 * @return Returns the geometryFactory.
	 */
	public GeometryFactory getGeometryFactory() {
		return geometryFactory;
	}

	/**
	 * @param geometryFactory The geometryFactory to set.
	 */
	public void setGeometryFactory(GeometryFactory geometryFactory) {
		this.geometryFactory = geometryFactory;
	}
	
	
}
