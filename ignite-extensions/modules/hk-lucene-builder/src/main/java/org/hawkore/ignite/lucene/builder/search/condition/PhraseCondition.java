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
 * A {@link Condition} implementation that matches documents containing a particular sequence of terms.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PhraseCondition extends Condition<PhraseCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The phrase terms to be matched. */
    @JsonProperty("value")
    final String value;

    /** The number of other words permitted between words in phrase. */
    @JsonProperty("slop")
    Integer slop;

    /**
     * Returns a new {@link PhraseCondition} with the specified field name and values to be matched.
     *
     * @param field the name of the field to be matched
     * @param value the phrase terms to be matched
     */
    @JsonCreator
    public PhraseCondition(@JsonProperty("field") String field, @JsonProperty("value") String value) {
        this.field = field;
        this.value = value;
    }

    /**
     * Sets the specified slop. Slop is the number of other words permitted between words in phrase.
     *
     * @param slop the number of other words permitted between words in phrase to set
     * @return this with the specified slop
     */
    public PhraseCondition slop(Integer slop) {
        this.slop = slop;
        return this;
    }
}
