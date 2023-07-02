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
package org.hawkore.ignite.lucene.search.condition.builder;

import org.hawkore.ignite.lucene.search.condition.GeoBBoxCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link GeoBBoxCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoBBoxConditionBuilder extends ConditionBuilder<GeoBBoxCondition, GeoBBoxConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The minimum accepted latitude. */
    @JsonProperty("min_latitude")
    private final double minLatitude;

    /** The maximum accepted latitude. */
    @JsonProperty("max_latitude")
    private final double maxLatitude;

    /** The minimum accepted longitude. */
    @JsonProperty("min_longitude")
    private final double minLongitude;

    /** The maximum accepted longitude. */
    @JsonProperty("max_longitude")
    private final double maxLongitude;

    /**
     * Returns a new {@link GeoBBoxConditionBuilder} with the specified field name and bounding box coordinates.
     *
     * @param field the name of the field to be matched
     * @param minLatitude the minimum accepted latitude
     * @param maxLatitude the maximum accepted latitude
     * @param minLongitude the minimum accepted longitude
     * @param maxLongitude the maximum accepted longitude
     */
    @JsonCreator
    public GeoBBoxConditionBuilder(@JsonProperty("field") String field,
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

    /**
     * Returns the {@link GeoBBoxCondition} represented by this builder.
     *
     * @return a new geo bounding box condition
     */
    @Override
    public GeoBBoxCondition build() {
        return new GeoBBoxCondition(boost, field, minLatitude, maxLatitude, minLongitude, maxLongitude);
    }
}
