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
package org.hawkore.ignite.lucene.search.condition;

import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.common.GeoOperation;
import org.hawkore.ignite.lucene.common.GeoShape;
import org.hawkore.ignite.lucene.common.GeoTransformation;
import org.hawkore.ignite.lucene.schema.Schema;
import org.hawkore.ignite.lucene.schema.mapping.GeoPointMapper;
import org.hawkore.ignite.lucene.schema.mapping.GeoShapeMapper;
import org.hawkore.ignite.lucene.schema.mapping.Mapper;

import com.google.common.base.MoreObjects;

/**
 * {@link Condition} that matches documents related to a JTS geographical shape. It is possible to apply a sequence of
 * {@link GeoTransformation}s to the provided shape to search for points related to the resulting shape.
 *
 * The shapes are defined using the <a href="http://en.wikipedia.org/wiki/Well-known_text"> Well Known Text (WKT)</a>
 * format.
 *
 * <p> This class depends on <a href="https://projects.eclipse.org/projects/locationtech.jts">Java Topology
 * Suite (JTS)</a>. This library can't be distributed together with this project due to license compatibility problems,
 * but you can add it by putting <a href="http://search.maven.org/remotecontent?filepath=org/locationtech/jts/jts-core/1.15.0/jts-core-1.15.0.jar">jts-core-1.15.0.jar</a>
 * into project lib directory.
 *
 * Pole wrapping is not supported.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeCondition extends SingleFieldCondition {

    /** The default spatial operation. */
    public static final GeoOperation DEFAULT_OPERATION = GeoOperation.IS_WITHIN;

    /** The shape. */
    public final GeoShape shape;

    /** The spatial operation to be applied. */
    public final GeoOperation operation;

    /**
     * Constructor receiving the shape, the spatial operation to be done and the transformations to be applied.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the field name
     * @param shape the shape
     * @param operation the spatial operation to be done, defaults to {@link #DEFAULT_OPERATION}
     */
    public GeoShapeCondition(Float boost, String field, GeoShape shape, GeoOperation operation) {
        super(boost, field);

        if (shape == null) {
            throw new IndexException("Shape required");
        }

        this.shape = shape;
        this.operation = operation == null ? DEFAULT_OPERATION : operation;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(Schema schema) {

        // Get the spatial strategy from the mapper
        SpatialStrategy strategy;
        Mapper mapper = schema.mapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for field '{}'", field);
        } else if (mapper instanceof GeoShapeMapper) {
            strategy = ((GeoShapeMapper) mapper).strategy;
        } else if (mapper instanceof GeoPointMapper) {
            strategy = ((GeoPointMapper) mapper).strategy;
        } else {
            throw new IndexException("'geo_shape' search requires a mapper of type 'geo_point' or 'geo_shape' " +
                                     "but found {}:{}", field, mapper);
        }

        // Build query
        SpatialArgs args = new SpatialArgs(operation.getSpatialOperation(), shape.apply());
        args.setDistErr(0.0);
        return strategy.makeQuery(args);
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("shape", shape).add("operation", operation);
    }
}