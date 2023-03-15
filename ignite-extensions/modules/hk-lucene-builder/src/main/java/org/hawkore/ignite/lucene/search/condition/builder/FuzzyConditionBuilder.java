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

import org.hawkore.ignite.lucene.search.condition.FuzzyCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link FuzzyCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FuzzyConditionBuilder extends ConditionBuilder<FuzzyCondition, FuzzyConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The fuzzy expression to be matched. */
    @JsonProperty("value")
    private final String value;

    /** The Damerau-Levenshtein max distance. */
    @JsonProperty("max_edits")
    private Integer maxEdits;

    /** The length of common (non-fuzzy) prefix. */
    @JsonProperty("prefix_length")
    private Integer prefixLength;

    /** The maximum number of terms to match. */
    @JsonProperty("max_expansions")
    private Integer maxExpansions;

    /** If transpositions should be treated as a primitive edit operation. */
    @JsonProperty("transpositions")
    private Boolean transpositions;

    /**
     * Returns a new {@link FuzzyConditionBuilder}.
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    @JsonCreator
    public FuzzyConditionBuilder(@JsonProperty("field") String field, @JsonProperty("value") String value) {
        this.field = field;
        this.value = value;
    }

    /**
     * Returns this builder with the specified Damerau-Levenshtein max distance.
     *
     * @param maxEdits the Damerau-Levenshtein max distance
     * @return this with the specified Damerau-Levenshtein max distance
     */
    public FuzzyConditionBuilder maxEdits(Integer maxEdits) {
        this.maxEdits = maxEdits;
        return this;
    }

    /**
     * Returns this builder with the length of common (non-fuzzy) prefix.
     *
     * @param prefixLength the length of common (non-fuzzy) prefix
     * @return this with the specified prefix length
     */
    public FuzzyConditionBuilder prefixLength(Integer prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * Returns this builder with the specified maximum number of terms to match.
     *
     * @param maxExpansions the maximum number of terms to match
     * @return the specified max expansions
     */
    public FuzzyConditionBuilder maxExpansions(Integer maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }

    /**
     * Returns this builder with the specified  if transpositions should be treated as a primitive edit operation.
     *
     * @param transpositions If transpositions should be treated as a primitive edit operation.
     * @return This builder with the specified  if transpositions should be treated as a primitive edit operation.
     */
    public FuzzyConditionBuilder transpositions(Boolean transpositions) {
        this.transpositions = transpositions;
        return this;
    }

    /**
     * Returns the {@link FuzzyCondition} represented by this builder.
     *
     * @return The {@link FuzzyCondition} represented by this builder.
     */
    @Override
    public FuzzyCondition build() {
        return new FuzzyCondition(boost, field, value, maxEdits, prefixLength, maxExpansions, transpositions);
    }
}
