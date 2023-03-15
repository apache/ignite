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

import org.hawkore.ignite.lucene.search.condition.RegexpCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link RegexpCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RegexpConditionBuilder extends ConditionBuilder<RegexpCondition, RegexpConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The wildcard expression to be matched. */
    @JsonProperty("value")
    private final String value;

    /**
     * Creates a new {@link RegexpConditionBuilder} for the specified field and expression.
     *
     * @param field the name of the field to be matched
     * @param value the wildcard expression to be matched
     */
    @JsonCreator
    public RegexpConditionBuilder(@JsonProperty("field") String field, @JsonProperty("value") String value) {
        this.field = field;
        this.value = value;
    }

    /**
     * Returns the {@link RegexpCondition} represented by this builder.
     *
     * @return a new regexp condition
     */
    @Override
    public RegexpCondition build() {
        return new RegexpCondition(boost, field, value);
    }
}
