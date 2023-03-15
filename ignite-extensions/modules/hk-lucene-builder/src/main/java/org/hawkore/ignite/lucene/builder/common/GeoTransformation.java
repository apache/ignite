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

import org.hawkore.ignite.lucene.builder.JSONBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoTransformation.BBox.class, name = "bbox"),
               @JsonSubTypes.Type(value = GeoTransformation.Buffer.class, name = "buffer"),
               @JsonSubTypes.Type(value = GeoTransformation.Centroid.class, name = "centroid"),
               @JsonSubTypes.Type(value = GeoTransformation.ConvexHull.class, name = "convex_hull")})
public abstract class GeoTransformation extends JSONBuilder {

    /**
     * {@link GeoTransformation} that gets the bounding box of a JTS geographical shape. The bounding box of shape is
     * the minimal rectangle containing the shape.
     */
    public static class BBox extends GeoTransformation {

    }

    /**
     * {@link GeoTransformation} for getting the buffer around a JTS geographical shape.
     */
    public static class Buffer extends GeoTransformation {

        /** The max allowed distance. */
        @JsonProperty("max_distance")
        String maxDistance;

        /** The min allowed distance. */
        @JsonProperty("min_distance")
        String minDistance;

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
     * {@link GeoTransformation} that gets the center point of a JTS geographical shape.
     */
    public static class Centroid extends GeoTransformation {

    }

    /**
     * {@link GeoTransformation} that gets the convex hull of a JTS geographical shape.
     */
    public static class ConvexHull extends GeoTransformation {

    }

}

