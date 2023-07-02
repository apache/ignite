/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.builder.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hawkore.ignite.lucene.builder.JSONBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoShape.WKT.class, name = "wkt"),
               @JsonSubTypes.Type(value = GeoShape.BBox.class, name = "bbox"),
               @JsonSubTypes.Type(value = GeoShape.Buffer.class, name = "buffer"),
               @JsonSubTypes.Type(value = GeoShape.Centroid.class, name = "centroid"),
               @JsonSubTypes.Type(value = GeoShape.ConvexHull.class, name = "convex_hull"),
               @JsonSubTypes.Type(value = GeoShape.Difference.class, name = "difference"),
               @JsonSubTypes.Type(value = GeoShape.Intersection.class, name = "intersection"),
               @JsonSubTypes.Type(value = GeoShape.Union.class, name = "union")})
public abstract class GeoShape extends JSONBuilder {

    /**
     * {@link GeoShape} in WKT format.
     */
    public static class WKT extends GeoShape {

        /** The WKT representation of the shape. **/
        @JsonProperty("value")
        final String value;

        /**
         * Constructor taking the WKT representation of the shape.
         *
         * @param value the WKT value
         */
        public WKT(String value) {
            this.value = value;
        }

    }

    /**
     * {@link GeoShape} that gets the bounding box of a geographical shape. The bounding box of shape is
     * the minimal rectangle containing the shape.
     */
    public static class BBox extends GeoShape {

        /** The base shape. **/
        @JsonProperty("shape")
        final GeoShape shape;

        /**
         * Constructor taking the shape.
         *
         * @param shape a shape
         */
        public BBox(GeoShape shape) {
            this.shape = shape;
        }

    }

    /**
     * {@link GeoShape} for getting the buffer around a geographical shape.
     */
    public static class Buffer extends GeoShape {

        /** The shape to be buffered. **/
        @JsonProperty("shape")
        final GeoShape shape;

        /** The max allowed distance. */
        @JsonProperty("max_distance")
        String maxDistance;

        /** The min allowed distance. */
        @JsonProperty("min_distance")
        String minDistance;

        /**
         * Constructor taking the shape.
         *
         * @param shape a shape
         */
        public Buffer(GeoShape shape) {
            this.shape = shape;
        }

        /**
         * Sets the max allowed distance.
         *
         * @param maxDistance the max distance
         * @return this with the specified max distance
         */
        public Buffer maxDistance(String maxDistance) {
            this.maxDistance = maxDistance;
            return this;
        }

        /**
         * Sets the min allowed distance.
         *
         * @param minDistance the min distance
         * @return this with the specified min distance
         */
        public Buffer minDistance(String minDistance) {
            this.minDistance = minDistance;
            return this;
        }
    }

    /**
     * {@link GeoShape} that gets the center point of a geographical shape.
     */
    public static class Centroid extends GeoShape {

        /** The shape. **/
        @JsonProperty("shape")
        final GeoShape shape;

        /**
         * Constructor taking the shape.
         *
         * @param shape a shape
         */
        public Centroid(GeoShape shape) {
            this.shape = shape;
        }

    }

    /**
     * {@link GeoShape} that gets the convex hull of a geographical shape.
     */
    public static class ConvexHull extends GeoShape {

        /** The shape. **/
        @JsonProperty("shape")
        final GeoShape shape;

        /**
         * Constructor taking the shape.
         *
         * @param shape a shape
         */
        public ConvexHull(GeoShape shape) {
            this.shape = shape;
        }

    }

    /**
     * {@link GeoShape} that gets the difference of two geographical shapes.
     */
    public static class Difference extends GeoShape {

        /** The shapes to be subtracted. */
        @JsonProperty("shapes")
        public final List<GeoShape> shapes;

        /**
         * Constructor receiving the shapes to be subtracted.
         *
         * @param shapes the shapes to be subtracted
         */
        public Difference(GeoShape... shapes) {
            this(new ArrayList<>(Arrays.asList(shapes)));
        }

        /**
         * Constructor receiving the shapes to be subtracted.
         *
         * @param shapes the shapes to be subtracted
         */
        public Difference(List<GeoShape> shapes) {
            this.shapes = shapes;
        }

        /**
         * Adds the specified {@link GeoShape} to the shapes to be subtracted.
         *
         * @param shape the shape to be added
         * @return this with the specified shape
         */
        public Difference add(GeoShape shape) {
            shapes.add(shape);
            return this;
        }

        /**
         * Adds the specified {@link GeoShape} to the shapes to be subtracted.
         *
         * @param shape the shape to be added in WKT format
         * @return this with the specified shape
         */
        public Difference add(String shape) {
            shapes.add(new WKT(shape));
            return this;
        }

    }

    /**
     * {@link GeoShape} that gets the intersection of two geographical shapes.
     */
    public static class Intersection extends GeoShape {

        /** The shapes to be intersected. */
        @JsonProperty("shapes")
        public final List<GeoShape> shapes;

        /**
         * Constructor receiving the shapes to be intersected.
         *
         * @param shapes the shapes to be intersected
         */
        public Intersection(GeoShape... shapes) {
            this(new ArrayList<>(Arrays.asList(shapes)));
        }

        /**
         * Constructor receiving the shapes to be intersected.
         *
         * @param shapes the shapes to be intersected
         */
        public Intersection(List<GeoShape> shapes) {
            this.shapes = shapes;
        }

        /**
         * Adds the specified {@link GeoShape} to the shapes to be intersected.
         *
         * @param shape the shape to be added
         * @return this with the specified shape
         */
        public Intersection add(GeoShape shape) {
            shapes.add(shape);
            return this;
        }

        /**
         * Adds the specified {@link GeoShape} to the shapes to be intersected.
         *
         * @param shape the shape to be added in WKT format
         * @return this with the specified shape
         */
        public Intersection add(String shape) {
            shapes.add(new WKT(shape));
            return this;
        }

    }

    /**
     * {@link GeoShape} that gets the union of two geographical shapes.
     */
    public static class Union extends GeoShape {

        /** The shapes to be added. */
        @JsonProperty("shapes")
        public final List<GeoShape> shapes;

        /**
         * Constructor receiving the shapes to be added.
         *
         * @param shapes the shapes to be added
         */
        public Union(GeoShape... shapes) {
            this(new ArrayList<>(Arrays.asList(shapes)));
        }

        /**
         * Constructor receiving the shapes to be added.
         *
         * @param shapes the shapes to be added
         */
        public Union(List<GeoShape> shapes) {
            this.shapes = shapes;
        }

        /**
         * Adds the specified {@link GeoShape} to the shapes to be added.
         *
         * @param shape the shape to be added
         * @return this with the specified shape
         */
        public Union add(GeoShape shape) {
            shapes.add(shape);
            return this;
        }

        /**
         * Adds the specified {@link GeoShape} to the shapes to be added.
         *
         * @param shape the shape to be added in WKT format
         * @return this with the specified shape
         */
        public Union add(String shape) {
            shapes.add(new WKT(shape));
            return this;
        }

    }

}

