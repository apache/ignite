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
package org.hawkore.ignite.lucene.builder.search.sort;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A geo spatial distance search sort.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {

    /** The name of the geo point field mapper to use to calculate distance. */
    @JsonProperty("field")
    final String field;

    /** The latitude of the center point to sort by distance to it. */
    @JsonProperty("latitude")
    final double latitude;

    /** The longitude of the center point to sort by distance to it. */
    @JsonProperty("longitude")
    final double longitude;

    /**
     * Creates a new {@link GeoDistanceSortField} for the specified field and reverse option.
     *
     * @param field the name of the geo point field mapper to be used for sorting
     * @param latitude the latitude in degrees of the reference point
     * @param longitude the longitude in degrees of the reference point
     */
    @JsonCreator
    public GeoDistanceSortField(@JsonProperty("field") String field,
                                @JsonProperty("latitude") double latitude,
                                @JsonProperty("longitude") double longitude) {
        this.field = field;
        this.latitude = latitude;
        this.longitude = longitude;
    }
}