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
package org.hawkore.ignite.lucene.schema.mapping;

import static org.hawkore.ignite.lucene.common.GeospatialUtils.CONTEXT;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.column.Columns;
import org.hawkore.ignite.lucene.common.GeospatialUtils;
import org.locationtech.spatial4j.shape.Point;

import com.google.common.base.MoreObjects;

/**
 * A {@link Mapper} to map geographical points.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoPointMapper extends MultipleColumnMapper {

    /** The default max number of levels for geohash search trees. */
    public static final int DEFAULT_MAX_LEVELS = 11;

    /** The name of the latitude column. */
    public final String latitude;

    /** The name of the longitude column. */
    public final String longitude;

    /** The max number of levels in the tree. */
    public final int maxLevels;

    /** The spatial strategy. */
    public final CompositeSpatialStrategy strategy;

    /**
     * Builds a new {@link GeoPointMapper}.
     *
     * @param field the name of the field
     * @param validated if the field must be validated
     * @param latitude the name of the column containing the latitude
     * @param longitude the name of the column containing the longitude
     * @param maxLevels the maximum number of levels in the geohash search tree. False positives will be discarded using
     * stored doc values, so a low value doesn't mean precision lost. High values will produce few false positives to be
     * post-filtered, at the expense of creating more terms in the search index.
     */
    public GeoPointMapper(String field, Boolean validated, String latitude, String longitude, Integer maxLevels) {
        super(field, validated, Arrays.asList(latitude, longitude), NUMERIC_TYPES, Collections.singletonList(Byte.class));

        if (StringUtils.isBlank(latitude)) {
            throw new IndexException("latitude column name is required");
        }

        if (StringUtils.isBlank(longitude)) {
            throw new IndexException("longitude column name is required");
        }

        this.latitude = latitude;
        this.longitude = longitude;
        this.maxLevels = GeospatialUtils.validateGeohashMaxLevels(maxLevels, DEFAULT_MAX_LEVELS);

        SpatialPrefixTree grid = new GeohashPrefixTree(CONTEXT, this.maxLevels);
        RecursivePrefixTreeStrategy indexStrategy = new RecursivePrefixTreeStrategy(grid, field);
        SerializedDVStrategy geometryStrategy = new SerializedDVStrategy(CONTEXT, field);
        strategy = new CompositeSpatialStrategy(field, indexStrategy, geometryStrategy);
    }

    /** {@inheritDoc} */
    @Override
    public List<IndexableField> indexableFields(Columns columns) {

        Double lon = readLongitude(columns);
        Double lat = readLatitude(columns);

        if (lon == null && lat == null) {
            return Collections.emptyList();
        } else if (lat == null) {
            throw new IndexException("Latitude column required if there is a longitude");
        } else if (lon == null) {
            throw new IndexException("Longitude column required if there is a latitude");
        }

        Point point = CONTEXT.makePoint(lon, lat);

        return Arrays.asList(strategy.createIndexableFields(point));
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        throw new IndexException("GeoPoint mapper '{}' does not support simple sorting", name);
    }

    /**
     * Returns the latitude contained in the specified {@link Columns}. A valid latitude must in the range [-90, 90].
     *
     * @param columns the columns containing the latitude
     * @return the validated latitude
     */
    Double readLatitude(Columns columns) {
        Object value = columns.valueForField(latitude);
        return value == null ? null : readLatitude(value);
    }

    /**
     * Returns the longitude contained in the specified {@link Columns}. A valid longitude must in the range [-180,
     * 180].
     *
     * @param columns the columns containing the longitude
     * @return the validated longitude
     */
    Double readLongitude(Columns columns) {
        Object value = columns.valueForField(longitude);
        return value == null ? null : readLongitude(value);
    }

    /**
     * Returns the latitude contained in the specified {@link Object}.
     *
     * A valid latitude must in the range [-90, 90].
     *
     * @param o the {@link Object} containing the latitude
     * @return the latitude
     */
    private static Double readLatitude(Object o) {
        if (o == null) {
            return null;
        }
        Double value;
        if (o instanceof Number) {
            value = ((Number) o).doubleValue();
        } else {
            try {
                value = Double.valueOf(o.toString());
            } catch (NumberFormatException e) {
                throw new IndexException("Unparseable latitude: {}", o);
            }
        }
        return GeospatialUtils.checkLatitude("latitude", value);
    }

    /**
     * Returns the longitude contained in the specified {@link Object}.
     *
     * A valid longitude must in the range [-180, 180].
     *
     * @param o the {@link Object} containing the latitude
     * @return the longitude
     */
    private static Double readLongitude(Object o) {
        if (o == null) {
            return null;
        }
        Double value;
        if (o instanceof Number) {
            value = ((Number) o).doubleValue();
        } else {
            try {
                value = Double.valueOf(o.toString());
            } catch (NumberFormatException e) {
                throw new IndexException("Unparseable longitude: {}", o);
            }
        }
        return GeospatialUtils.checkLongitude("longitude", value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("field", field)
                          .add("validated", validated)
                          .add("latitude", latitude)
                          .add("longitude", longitude)
                          .add("maxLevels", maxLevels)
                          .toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((latitude == null) ? 0 : latitude.hashCode());
        result = prime * result + ((longitude == null) ? 0 : longitude.hashCode());
        result = prime * result + maxLevels;
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        GeoPointMapper other = (GeoPointMapper) obj;
        if (latitude == null) {
            if (other.latitude != null)
                return false;
        } else if (!latitude.equals(other.latitude))
            return false;
        if (longitude == null) {
            if (other.longitude != null)
                return false;
        } else if (!longitude.equals(other.longitude))
            return false;
        if (maxLevels != other.maxLevels)
            return false;
        return true;
    }
    
    
}
