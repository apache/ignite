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

import org.hawkore.ignite.lucene.search.condition.RangeCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link RangeCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RangeConditionBuilder extends ConditionBuilder<RangeCondition, RangeConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The lower accepted value. Maybe {@code null} meaning no lower limit. */
    @JsonProperty("lower")
    private Object lower;

    /** The upper accepted value. Maybe {@code null} meaning no upper limit. */
    @JsonProperty("upper")
    private Object upper;

    /** If the lower value must be included if not {@code null}. */
    @JsonProperty("include_lower")
    private Boolean includeLower;

    /** If the upper value must be included if not {@code null}. */
    @JsonProperty("include_upper")
    private Boolean includeUpper;

    /** If the generated query should use doc values. */
    @JsonProperty("doc_values")
    private Boolean docValues;

    /**
     * Creates a new {@link RangeConditionBuilder} for the specified field.
     *
     * @param field the name of the field to be matched
     */
    @JsonCreator
    public RangeConditionBuilder(@JsonProperty("field") String field) {
        this.field = field;
    }

    /**
     * Sets the lower value to be matched.
     *
     * @param lower the lower value to be matched, or {@code null} if there is no lower limit
     * @return this with the specified lower value to be matched
     */
    public RangeConditionBuilder lower(Object lower) {
        this.lower = lower;
        return this;
    }

    /**
     * Sets the upper value to be matched.
     *
     * @param upper the lower value to be matched, or {@code null} if there is no upper limit
     * @return this with the specified upper value to be matched
     */
    public RangeConditionBuilder upper(Object upper) {
        this.upper = upper;
        return this;
    }

    /**
     * Sets if the lower value must be included.
     *
     * @param includeLower {@code true} the lower value must be included, {@code false} otherwise
     * @return this with the specified lower interval option
     */
    public RangeConditionBuilder includeLower(Boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Sets if the upper value must be included.
     *
     * @param includeUpper if the upper value must be included
     * @return this builder with the specified upper interval option
     */
    public RangeConditionBuilder includeUpper(Boolean includeUpper) {
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
    public RangeConditionBuilder docValues(Boolean docValues) {
        this.docValues = docValues;
        return this;
    }

    /**
     * Returns the {@link RangeCondition} represented by this builder.
     *
     * @return a new range condition
     */
    @Override
    public RangeCondition build() {
        return new RangeCondition(boost, field, lower, upper, includeLower, includeUpper, docValues);
    }
}
