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
public class RangeCondition extends Condition<RangeCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The lower accepted value. Maybe null meaning no lower limit. */
    @JsonProperty("lower")
    Object lower;

    /** The upper accepted value. Maybe null meaning no upper limit. */
    @JsonProperty("upper")
    Object upper;

    /** If the lower value must be included if not {@code null}. */
    @JsonProperty("include_lower")
    Boolean includeLower;

    /** If the upper value must be included if not {@code null}. */
    @JsonProperty("include_upper")
    Boolean includeUpper;

    /** If the generated query should use doc values. */
    @JsonProperty("doc_values")
    Boolean docValues;

    /**
     * Creates a new {@link RangeCondition} for the specified field.
     *
     * @param field the name of the field to be matched
     */
    @JsonCreator
    public RangeCondition(@JsonProperty("field") String field) {
        this.field = field;
    }

    /**
     * Sets the lower value to be matched, if any.
     *
     * @param lower the lower value to be matched, or {@code null} if there is no lower limit
     * @return this with the specified lower value to be matched
     */
    public RangeCondition lower(Object lower) {
        this.lower = lower;
        return this;
    }

    /**
     * Sets the upper value to be matched, if any.
     *
     * @param upper the lower value to be matched, or {@code null} if there is no upper limit
     * @return this with the specified upper value to be matched
     */
    public RangeCondition upper(Object upper) {
        this.upper = upper;
        return this;
    }

    /**
     * Sets if the lower value must be included.
     *
     * @param includeLower {@code true} if the lower value must be included, {@code false} otherwise
     * @return this with the specified lower interval option
     */
    public RangeCondition includeLower(Boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Sets if the upper value must be included.
     *
     * @param includeUpper {@code true} if the upper value must be included, {@code false} otherwise
     * @return this with the specified upper interval option
     */
    public RangeCondition includeUpper(Boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Sets if the generated query should use doc values. Doc values queries are typically slower, but they can be
     * faster in the dense case where most rows match the search.
     *
     * @param docValues if the generated query should use doc values
     * @return this builder with the specified use doc values option.
     */
    public RangeCondition docValues(Boolean docValues) {
        this.docValues = docValues;
        return this;
    }
}
