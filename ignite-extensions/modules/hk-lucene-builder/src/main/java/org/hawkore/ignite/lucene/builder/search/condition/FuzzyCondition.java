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
 * A {@link Condition} that implements the fuzzy search query. The similarity measurement is based on the
 * Damerau-Levenshtein (optimal string alignment) algorithm, though you can explicitly choose classic Levenshtein by
 * passing {@code false} to the {@code transpositions} parameter.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FuzzyCondition extends Condition<FuzzyCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The fuzzy expression to be matched. */
    @JsonProperty("value")
    final String value;

    /** The Damerau-Levenshtein max distance. */
    @JsonProperty("max_edits")
    Integer maxEdits;

    /** The length of common (non-fuzzy) prefix. */
    @JsonProperty("prefix_length")
    Integer prefixLength;

    /** The maximum number of terms to match. */
    @JsonProperty("max_expansions")
    Integer maxExpansions;

    /** If transpositions should be treated as a primitive edit operation. */
    @JsonProperty("transpositions")
    Boolean transpositions;

    /**
     * Returns a new {@link FuzzyCondition}.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     */
    @JsonCreator
    public FuzzyCondition(@JsonProperty("field") String field, @JsonProperty("value") String value) {
        this.field = field;
        this.value = value;
    }

    /**
     * Sets the specified Damerau-Levenshtein max distance.
     *
     * @param maxEdits the Damerau-Levenshtein max distance
     * @return this with the specified max edits
     */
    public FuzzyCondition maxEdits(Integer maxEdits) {
        this.maxEdits = maxEdits;
        return this;
    }

    /**
     * Sets the length of common (non-fuzzy) prefix.
     *
     * @param prefixLength the length of common (non-fuzzy) prefix
     * @return this with the specified prefix length
     */
    public FuzzyCondition prefixLength(Integer prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * Sets the specified maximum number of terms to match.
     *
     * @param maxExpansions the maximum number of terms to match
     * @return this with the specified max expansions
     */
    public FuzzyCondition maxExpansions(Integer maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }

    /**
     * Sets the specified if transpositions should be treated as a primitive edit operation.
     *
     * @param transpositions if transpositions should be treated as a primitive edit operation
     * @return this with the specified transpositions option
     */
    public FuzzyCondition transpositions(Boolean transpositions) {
        this.transpositions = transpositions;
        return this;
    }
}
