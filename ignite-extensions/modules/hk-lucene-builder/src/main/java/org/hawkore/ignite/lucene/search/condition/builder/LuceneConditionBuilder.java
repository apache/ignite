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

import org.hawkore.ignite.lucene.search.condition.LuceneCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link LuceneCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class LuceneConditionBuilder extends ConditionBuilder<LuceneCondition, LuceneConditionBuilder> {

    /** The Lucene query syntax expression. */
    @JsonProperty("query")
    private final String query;

    /** The name of the field where the clauses will be applied by default. */
    @JsonProperty("default_field")
    private String defaultField;

    /**
     * Returns a new {@link LuceneConditionBuilder} with the specified query.
     *
     * @param query the Lucene query syntax expression
     */
    @JsonCreator
    public LuceneConditionBuilder(@JsonProperty("query") String query) {
        this.query = query;
    }

    /**
     * Returns this builder with the specified default field name. This is the field where the clauses will be applied
     * by default.
     *
     * @param defaultField the name of the field where the clauses will be applied by default
     * @return this with the specified name of the default field
     */
    public LuceneConditionBuilder defaultField(String defaultField) {
        this.defaultField = defaultField;
        return this;
    }

    /**
     * Returns the {@link LuceneCondition} represented by this builder.
     *
     * @return a new Lucene syntax condition
     */
    @Override
    public LuceneCondition build() {
        return new LuceneCondition(boost, defaultField, query);
    }
}
