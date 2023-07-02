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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.mapping.SingleColumnMapper;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches documents containing a particular sequence of terms.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PhraseCondition extends SingleColumnCondition {

    /** The default umber of other words permitted between words in phrase. */
    public static final int DEFAULT_SLOP = 0;

    /** The phrase terms to be matched. */
    public final String value;

    /** The number of other words permitted between words in phrase. */
    public final int slop;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param value the phrase terms to be matched
     * @param slop the number of other words permitted between words in phrase
     */
    public PhraseCondition(Float boost, String field, String value, Integer slop) {
        super(boost, field);

        if (value == null) {
            throw new IndexException("Field value required");
        }
        if (slop != null && slop < 0) {
            throw new IndexException("Slop must be positive");
        }

        this.value = value;
        this.slop = slop == null ? DEFAULT_SLOP : slop;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        if (mapper.base == String.class) {
            QueryBuilder queryBuilder = new QueryBuilder(analyzer);
            Query query = queryBuilder.createPhraseQuery(field, value, slop);
            if (query == null) {
                query = new BooleanQuery.Builder().build();
            }
            return query;
        } else {
            throw new IndexException("Phrase queries are not supported by mapper '{}'", mapper);
        }
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("value", value).add("slop", slop);
    }
}