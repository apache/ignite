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

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.Schema;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches documents satisfying a Lucene Query Syntax.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneCondition extends Condition {

    /** The default name of the field where the clauses will be applied by default. */
    public static final String DEFAULT_FIELD = "lucene";

    /** The Lucene query syntax expression. */
    public final String query;

    /** The name of the field where the clauses will be applied by default. */
    public final String defaultField;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param defaultField the default field name
     * @param query the Lucene Query Syntax query
     */
    public LuceneCondition(Float boost, String defaultField, String query) {
        super(boost);
        if (StringUtils.isBlank(query)) {
            throw new IndexException("Query statement required");
        }
        this.query = query;
        this.defaultField = defaultField == null ? DEFAULT_FIELD : defaultField;
    }

    /** {@inheritDoc} */
    public Set<String> postProcessingFields() {
        Set<String> fields = new LinkedHashSet<>();
        if (!StringUtils.isBlank(defaultField)) {
            fields.add(defaultField);
        }
        for (String term : query.split("[ ]")) {
            if (term.contains(":")) {
                fields.add(term.split(":")[0]);
            }
        }
        return fields;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(Schema schema) {
        try {
            Analyzer analyzer = schema.analyzer;
            QueryParser queryParser = new QueryParser(defaultField, analyzer);
            queryParser.setAllowLeadingWildcard(true);
            return queryParser.parse(query);
        } catch (ParseException e) {
            throw new IndexException("Error while parsing lucene syntax query: {}", e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("query", query).add("defaultField", defaultField);
    }
}