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

import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.mapping.SingleColumnMapper;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches documents containing a value for a field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ContainsCondition extends SingleColumnCondition {

    /** The default use doc values option. */
    public static final boolean DEFAULT_DOC_VALUES = false;

    /** The name of the field to be matched. */
    public final String field;

    /** The value of the field to be matched. */
    public final Object[] values;

    /** If the generated query should use doc values. */
    public final boolean docValues;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param values the value of the field to be matched
     * @param docValues if the generated query should use doc values
     */
    public ContainsCondition(Float boost, String field, Boolean docValues, Object... values) {
        super(boost, field);

        if (values == null || values.length == 0) {
            throw new IndexException("Field values required");
        }

        this.field = field;
        this.values = values;
        this.docValues = docValues == null ? DEFAULT_DOC_VALUES : docValues;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(SingleColumnMapper<?> mapper, Analyzer analyzer) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Object value : values) {
            MatchCondition condition = new MatchCondition(null, field, value, docValues);
            builder.add(condition.doQuery(mapper, analyzer), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("values", Arrays.toString(values)).add("docValues", docValues);
    }
}