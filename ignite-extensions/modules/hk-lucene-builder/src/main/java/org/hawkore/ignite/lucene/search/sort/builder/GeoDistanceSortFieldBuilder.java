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
package org.hawkore.ignite.lucene.search.sort.builder;

import org.hawkore.ignite.lucene.search.sort.GeoDistanceSortField;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortFieldBuilder extends SortFieldBuilder<GeoDistanceSortField, GeoDistanceSortFieldBuilder> {

    /** The name of the the geo point field mapper to use to calculate distance. */
    @JsonProperty("field")
    private final String field;

    /** The latitude of the center point to sort by min distance to it. */
    @JsonProperty("latitude")
    private final double latitude;

    /** The longitude of the center point to sort by min distance to it. */
    @JsonProperty("longitude")
    private final double longitude;

    /**
     * Creates a new {@link GeoDistanceSortFieldBuilder} for the specified field.
     *
     * @param field the name of the geo point field mapper to use to calculate distance
     * @param latitude the latitude of the center point to sort by min distance to it
     * @param longitude the longitude of the center point to sort by min distance to it
     */
    @JsonCreator
    public GeoDistanceSortFieldBuilder(@JsonProperty("field") String field,
                                       @JsonProperty("latitude") double latitude,
                                       @JsonProperty("longitude") double longitude) {

        this.field = field;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    /** {@inheritDoc} */
    @Override
    public GeoDistanceSortField build() {
        return new GeoDistanceSortField(field, reverse, latitude, longitude);
    }
}