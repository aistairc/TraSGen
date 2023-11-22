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
package DataGen.geometryGenerator;


import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;


/**
 * 
 * Cascades the effort of creating a set of topologically valid geometries.
 *
 * @author David Zwiers, Vivid Solutions. 
 */
public abstract class MultiGenerator extends GeometryGenerator {

	protected GeometryGenerator generator = null;
	protected int numberGeometries = 2;
	protected int generationAlgorithm = 0;
	
	/**
	 * Grid style blocks
	 */
	public static final int BOX = 0;
	/**
	 * vertical strips
	 */
	public static final int VERT = 1;
	/**
	 * Horizontal strips
	 */
	public static final int HORZ = 2;

	/**
	 * Random strips
	 */
	public static final int RND = 3;
	
	/**
	 * @param generator
	 */
	public MultiGenerator(GeometryGenerator generator) {
		this.generator = generator;
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
	public abstract Geometry create();

	/**
	 * @return Returns the numberGeometries.
	 */
	public int getNumberGeometries() {
		return numberGeometries;
	}

	/**
	 * @param numberGeometries The numberGeometries to set.
	 */
	public void setNumberGeometries(int numberGeometries) {
		this.numberGeometries = numberGeometries;
	}

	/**
	 * @return Returns the generator.
	 */
	public GeometryGenerator getGenerator() {
		return generator;
	}

	/**
	 * @see org.locationtech.jts.generator.GeometryGenerator#setBoundingBox(Envelope)
	 */
	public void setBoundingBox(Envelope boundingBox) {
		super.setBoundingBox(boundingBox);
		if(generator!=null)
			generator.setBoundingBox(boundingBox);
	}

	/**
	 * @see org.locationtech.jts.generator.GeometryGenerator#setDimensions(int)
	 */
	public void setDimensions(int dimensions) {
		super.setDimensions(dimensions);
		if(generator!=null)
			generator.setDimensions(dimensions);
	}

	/**
	 * @see org.locationtech.jts.generator.GeometryGenerator#setGeometryFactory(GeometryFactory)
	 */
	public void setGeometryFactory(GeometryFactory geometryFactory) {
		super.setGeometryFactory(geometryFactory);
		if(generator!=null)
			generator.setGeometryFactory(geometryFactory);
	}

	/**
	 * @param generationAlgorithm The generationAlgorithm to set.
	 */
	public void setGenerationAlgorithm(int generationAlgorithm) {
		this.generationAlgorithm = generationAlgorithm;
	}
}
