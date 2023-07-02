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
 * A {@link Condition} that matches documents containing a shape contained in a certain bounding box.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoBBoxCondition extends Condition<GeoBBoxCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The minimum accepted latitude. */
    @JsonProperty("min_latitude")
    final double minLatitude;

    /** The maximum accepted latitude. */
    @JsonProperty("max_latitude")
    final double maxLatitude;

    /** The minimum accepted longitude. */
    @JsonProperty("min_longitude")
    final double minLongitude;

    /** The maximum accepted longitude. */
    @JsonProperty("max_longitude")
    final double maxLongitude;

    /**
     * Returns a new {@link GeoBBoxCondition} with the specified field name and bounding box coordinates.
     *
     * @param field the name of the field to be matched
     * @param minLatitude the minimum accepted latitude
     * @param maxLatitude the maximum accepted latitude
     * @param minLongitude the minimum accepted longitude
     * @param maxLongitude the maximum accepted longitude
     */
    @JsonCreator
    public GeoBBoxCondition(@JsonProperty("field") String field,
                            @JsonProperty("min_latitude") double minLatitude,
                            @JsonProperty("max_latitude") double maxLatitude,
                            @JsonProperty("min_longitude") double minLongitude,
                            @JsonProperty("max_longitude") double maxLongitude) {
        this.field = field;
        this.minLongitude = minLongitude;
        this.maxLongitude = maxLongitude;
        this.minLatitude = minLatitude;
        this.maxLatitude = maxLatitude;
    }
}
