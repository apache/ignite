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

import static org.hawkore.ignite.lucene.common.GeospatialUtils.CONTEXT;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.common.GeospatialUtils;
import org.hawkore.ignite.lucene.schema.mapping.GeoPointMapper;
import org.hawkore.ignite.lucene.schema.mapping.GeoShapeMapper;
import org.hawkore.ignite.lucene.schema.mapping.Mapper;
import org.locationtech.spatial4j.shape.Rectangle;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} that matches documents containing a shape contained in a certain bounding box.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoBBoxCondition extends SingleMapperCondition<Mapper> {

    /** The minimum accepted latitude. */
    public final double minLatitude;

    /** The maximum accepted latitude. */
    public final double maxLatitude;

    /** The minimum accepted longitude. */
    public final double minLongitude;

    /** The maximum accepted longitude. */
    public final double maxLongitude;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param minLatitude the minimum accepted latitude
     * @param maxLatitude the maximum accepted latitude
     * @param minLongitude the minimum accepted longitude
     * @param maxLongitude the maximum accepted longitude
     */
    public GeoBBoxCondition(Float boost,
                            String field,
                            Double minLatitude,
                            Double maxLatitude,
                            Double minLongitude,
                            Double maxLongitude) {
        super(boost, field, Mapper.class);

        this.minLatitude = GeospatialUtils.checkLatitude("min_latitude", minLatitude);
        this.maxLatitude = GeospatialUtils.checkLatitude("max_latitude", maxLatitude);
        this.minLongitude = GeospatialUtils.checkLongitude("min_longitude", minLongitude);
        this.maxLongitude = GeospatialUtils.checkLongitude("max_longitude", maxLongitude);

        if (minLongitude > maxLongitude) {
            throw new IndexException("min_longitude must be lower than max_longitude");
        }

        if (minLatitude > maxLatitude) {
            throw new IndexException("min_latitude must be lower than max_latitude");
        }
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(Mapper mapper, Analyzer analyzer) {
    	
        SpatialStrategy spatialStrategy = null;
        
        if (mapper == null) {
            throw new IndexException("No mapper found for field '{}'", field);
        } else if (mapper instanceof GeoShapeMapper) {
        	spatialStrategy = ((GeoShapeMapper) mapper).strategy;
        } else if (mapper instanceof GeoPointMapper) {
        	spatialStrategy = ((GeoPointMapper) mapper).strategy;
        } else {
            throw new IndexException("'geo_box' search requires a mapper of type 'geo_point' or 'geo_shape' " +
                                     "but found {}:{}", field, mapper);
        }
    	
        Rectangle rectangle = CONTEXT.makeRectangle(minLongitude, maxLongitude, minLatitude, maxLatitude);
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, rectangle);
        return spatialStrategy.makeQuery(args);
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("minLatitude", minLatitude)
                                   .add("maxLatitude", maxLatitude)
                                   .add("minLongitude", minLongitude)
                                   .add("maxLongitude", maxLongitude);
    }
}