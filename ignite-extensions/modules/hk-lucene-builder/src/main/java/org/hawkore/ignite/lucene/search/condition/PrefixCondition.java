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
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.mapping.SingleColumnMapper;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches documents containing terms with a specified prefix.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PrefixCondition extends SingleColumnCondition {

    /** The field prefix to be matched. */
    public final String value;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param value the field prefix to be matched
     */
    public PrefixCondition(Float boost, String field, String value) {
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
            return new PrefixQuery(term);
        } else {
            throw new IndexException("Prefix queries are not supported by mapper '{}'", mapper);
        }
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("value", value);
    }
}