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
package org.hawkore.ignite.lucene.search.sort;

import static org.hawkore.ignite.lucene.common.GeospatialUtils.CONTEXT;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.common.GeospatialUtils;
import org.hawkore.ignite.lucene.schema.Schema;
import org.hawkore.ignite.lucene.schema.mapping.GeoPointMapper;
import org.hawkore.ignite.lucene.schema.mapping.GeoShapeMapper;
import org.hawkore.ignite.lucene.schema.mapping.Mapper;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

import com.google.common.base.MoreObjects;

/**
 * {@link SortField} to sort geo points by their distance to a fixed reference point.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {

    /** The name of mapper to use to calculate distance. */
    public final String field;

    /** The latitude of the center point to sort by min distance to it. */
    public final double latitude;

    /** The longitude of the center point to sort by min distance to it. */
    public final double longitude;

    /**
     * Returns a new {@link SortField}.
     *
     * @param field the name of the geo point field mapper to use to calculate distance
     * @param reverse {@code true} if natural order should be reversed
     * @param latitude the latitude
     * @param longitude the longitude
     */
    public GeoDistanceSortField(String field, Boolean reverse, double latitude, double longitude) {
        super(reverse);
        if (field == null || StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }
        this.field = field;
        this.latitude = GeospatialUtils.checkLatitude("latitude", latitude);
        this.longitude = GeospatialUtils.checkLongitude("longitude", longitude);
    }

    /** {@inheritDoc} */
    @Override
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        final Mapper mapper = schema.mapper(field);
        CompositeSpatialStrategy spatialStrategy = null;
        
        if (mapper == null) {
            throw new IndexException("No mapper found for sort field '{}'", field);
        } else if (mapper instanceof GeoShapeMapper) {
        	spatialStrategy = ((GeoShapeMapper) mapper).strategy;
        } else if (mapper instanceof GeoPointMapper) {
        	spatialStrategy = ((GeoPointMapper) mapper).strategy;
        } else {
            throw new IndexException("Spatial sort requires a mapper of type 'geo_point' or 'geo_shape'");
        }

        Point point = CONTEXT.makePoint(longitude, latitude);

        // Use the distance (in km) as source
        SpatialStrategy strategy = spatialStrategy.getGeometryStrategy();
        DoubleValuesSource valueSource = strategy.makeDistanceValueSource(point, DistanceUtils.DEG_TO_KM);
        return valueSource.getSortField(reverse);
    }

    /** {@inheritDoc} */
    public Set<String> postProcessingFields() {
        return Collections.singleton(field);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("reverse", reverse)
                          .add("latitude", latitude)
                          .add("longitude", longitude)
                          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeoDistanceSortField other = (GeoDistanceSortField) o;
        return reverse == other.reverse &&
               field.equals(other.field) &&
               latitude == other.latitude &&
               longitude == other.longitude;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        result = 31 * result + new Double(latitude).hashCode();
        result = 31 * result + new Double(longitude).hashCode();
        return result;
    }
}