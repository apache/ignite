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
package org.hawkore.ignite.lucene.search.condition;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.mapping.SingleColumnMapper;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} that implements the fuzzy search query. The similarity measurement is based on the
 * Damerau-Levenshtein (optimal string alignment) algorithm, though you can explicitly choose classic Levenshtein by
 * passing {@code false} to the {@code transpositions} parameter.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FuzzyCondition extends SingleColumnCondition {

    /** The default Damerau-Levenshtein max distance. */
    public static final int DEFAULT_MAX_EDITS = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;

    /** The default length of common (non-fuzzy) prefix. */
    public static final int DEFAULT_PREFIX_LENGTH = 0;

    /** The default max expansions. */
    public static final int DEFAULT_MAX_EXPANSIONS = 50;

    /** If transpositions should be treated as a primitive edit operation by default. */
    public static final boolean DEFAULT_TRANSPOSITIONS = true;

    /** The fuzzy expression to be matched. */
    public final String value;

    /** The Damerau-Levenshtein max distance. */
    public final int maxEdits;

    /** The length of common (non-fuzzy) prefix. */
    public final int prefixLength;

    /** The length of common (non-fuzzy) prefix. */
    public final int maxExpansions;

    /** If transpositions should be treated as a primitive edit operation. */
    public final boolean transpositions;

    /**
     * Returns a new {@link FuzzyCondition}.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the field name
     * @param value the field fuzzy value
     * @param maxEdits must be {@literal >=} 0 and {@literal <=} {@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
     * @param prefixLength length of common (non-fuzzy) prefix
     * @param maxExpansions The maximum number of terms to match. If this number is greater than {@link
     * org.apache.lucene.search.BooleanQuery#getMaxClauseCount} when the query is rewritten, then the maxClauseCount
     * will be used instead.
     * @param transpositions {@code true} if transpositions should be treated as a primitive edit operation. If this is
     * {@code false}, comparisons will implement the classic Levenshtein algorithm.
     */
    public FuzzyCondition(Float boost,
                          String field,
                          String value,
                          Integer maxEdits,
                          Integer prefixLength,
                          Integer maxExpansions,
                          Boolean transpositions) {
        super(boost, field);
        this.value = validateValue(value);
        this.maxEdits = validateMaxEdits(maxEdits);
        this.prefixLength = validatePrefixLength(prefixLength);
        this.maxExpansions = validateMaxExpansions(maxExpansions);
        this.transpositions = validateTranspositions(transpositions);
    }

    private static String validateValue(String value) {
        if (StringUtils.isBlank(value)) {
            throw new IndexException("Field value required");
        } else {
            return value;
        }
    }

    private static Integer validateMaxEdits(Integer maxEdits) {
        if (maxEdits == null) {
            return DEFAULT_MAX_EDITS;
        } else if (maxEdits < 0 || maxEdits > 2) {
            throw new IndexException("max_edits must be between 0 and 2");
        } else {
            return maxEdits;
        }
    }

    private static Integer validatePrefixLength(Integer prefixLength) {
        if (prefixLength == null) {
            return DEFAULT_PREFIX_LENGTH;
        } else if (prefixLength < 0) {
            throw new IndexException("prefix_length must be positive.");
        } else {
            return prefixLength;
        }
    }

    private static Integer validateMaxExpansions(Integer maxExpansions) {
        if (maxExpansions == null) {
            return DEFAULT_MAX_EXPANSIONS;
        } else if (maxExpansions < 0) {
            throw new IndexException("max_expansions must be positive.");
        } else {
            return maxExpansions;
        }
    }

    private static Boolean validateTranspositions(Boolean transpositions) {
        if (transpositions == null) {
            return DEFAULT_TRANSPOSITIONS;
        } else {
            return transpositions;
        }
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        if (mapper.base == String.class) {
            Term term = new Term(field, value);
            return new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
        } else {
            throw new IndexException("Fuzzy queries are not supported by mapper {}", mapper);
        }
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("value", value)
                                   .add("maxEdits", maxEdits)
                                   .add("prefixLength", prefixLength)
                                   .add("maxExpansions", maxExpansions)
                                   .add("transpositions", transpositions);
    }
}