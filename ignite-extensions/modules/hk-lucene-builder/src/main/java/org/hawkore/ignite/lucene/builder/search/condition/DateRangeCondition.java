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
 * A {@link Condition} implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeCondition extends Condition<DateRangeCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The lower accepted date. Maybe {@code null} meaning no lower limit. */
    @JsonProperty("from")
    Object from;

    /** The upper accepted date. Maybe {@code null} meaning no upper limit. */
    @JsonProperty("to")
    Object to;

    /** The spatial operation to be performed. */
    @JsonProperty("operation")
    String operation;

    /**
     * Returns a new {@link DateRangeCondition} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     */
    @JsonCreator
    public DateRangeCondition(@JsonProperty("field") String field) {
        this.field = field;
    }

    /**
     * Sets the lower accepted date. Maybe {@code null} meaning no lower limit.
     *
     * @param from the lower accepted date, or {@code null} if there is no lower limit
     * @return this with the specified lower accepted date
     */
    public DateRangeCondition from(Object from) {
        this.from = from;
        return this;
    }

    /**
     * Sets the upper accepted date. Maybe {@code null} meaning no upper limit.
     *
     * @param to the upper accepted date, or {@code null} if there is no upper limit
     * @return this with the specified upper accepted date
     */
    public DateRangeCondition to(Object to) {
        this.to = to;
        return this;
    }

    /**
     * Sets the spatial operation to be performed. Possible values are {@code intersects}, {@code is_within} and {@code
     * contains}. Defaults to {@code intersects}.
     *
     * @param operation the operation
     * @return this with the specified operation
     */
    public DateRangeCondition operation(String operation) {
        this.operation = operation;
        return this;
    }
}
