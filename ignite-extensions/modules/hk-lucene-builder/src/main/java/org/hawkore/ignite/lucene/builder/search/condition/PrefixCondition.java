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
 * A {@link Condition} implementation that matches documents containing terms with a specified prefix.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PrefixCondition extends Condition<PrefixCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The prefix to be matched. */
    @JsonProperty("value")
    final String value;

    /**
     * Creates a new {@link PrefixCondition}.
     *
     * @param field the name of the field to be matched
     * @param value the prefix to be matched
     */
    @JsonCreator
    public PrefixCondition(@JsonProperty("field") String field, @JsonProperty("value") String value) {
        this.field = field;
        this.value = value;
    }
}