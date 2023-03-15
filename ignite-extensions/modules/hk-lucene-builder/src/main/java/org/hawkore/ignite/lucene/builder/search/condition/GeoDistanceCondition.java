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
package org.hawkore.ignite.lucene.builder.search.condition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link Condition} that matches documents containing a point contained between two certain circles.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoDistanceCondition extends Condition<GeoDistanceCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The latitude of the reference point. */
    @JsonProperty("latitude")
    final double latitude;

    /** The longitude of the reference point. */
    @JsonProperty("longitude")
    final double longitude;

    /** The max allowed distance. */
    @JsonProperty("max_distance")
    final String maxDistance;

    /** The min allowed distance. */
    @JsonProperty("min_distance")
    String minDistance;

    /**
     * Returns a new {@link GeoDistanceCondition} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @param latitude the latitude of the reference point
     * @param longitude the longitude of the reference point
     * @param maxDistance the max allowed distance
     */
    @JsonCreator
    public GeoDistanceCondition(@JsonProperty("field") String field,
                                @JsonProperty("latitude") double latitude,
                                @JsonProperty("longitude") double longitude,
                                @JsonProperty("max_distance") String maxDistance) {
        this.field = field;
        this.longitude = longitude;
        this.latitude = latitude;
        this.maxDistance = maxDistance;
    }

    /**
     * Sets the min allowed distance.
     *
     * @param minDistance the min allowed distance
     * @return this
     */
    public GeoDistanceCondition minDistance(String minDistance) {
        this.minDistance = minDistance;
        return this;
    }
}
