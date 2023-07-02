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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.mapping.SingleColumnMapper;

import com.google.common.base.MoreObjects;

/**
 * Implements the wildcard search query. Supported wildcards are {@code *}, which matches any character sequence
 * (including the empty one), and {@code ?}, which matches any single character. '\' is the escape character.
 *
 * Note this query can be slow, as it needs to iterate over many terms. In order to prevent extremely slow
 * WildcardQueries, a Wildcard term should not start with the wildcard {@code *}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RegexpCondition extends SingleColumnCondition {

    /** The wildcard expression to be matched. */
    public final String value;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param value the wildcard expression to be matched
     */
    public RegexpCondition(Float boost, String field, String value) {
        super(boost, field);
        if (value == null) {
            throw new IndexException("Field value required");
        }
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        if (mapper.base == String.class) {
            Term term = new Term(field, value);
            return new RegexpQuery(term);
        } else {
            throw new IndexException("Regexp queries are not supported by mapper '{}'", mapper);
        }
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("value", value);
    }
}
