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

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.MUST_NOT;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.hawkore.ignite.lucene.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} that matches documents matching boolean combinations of other queries, e.g. {@link
 * MatchCondition}s, {@link RangeCondition}s or other {@link BooleanCondition}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BooleanCondition extends Condition {

    private static final Logger logger = LoggerFactory.getLogger(BooleanCondition.class);

    /** The mandatory conditions. */
    public final List<Condition> must;

    /** The optional conditions. */
    public final List<Condition> should;

    /** The mandatory not conditions. */
    public final List<Condition> not;

    /**
     * Returns a new {@link BooleanCondition} compound by the specified {@link Condition}s.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param must the mandatory conditions
     * @param should the optional conditions
     * @param not the mandatory not conditions
     */
    public BooleanCondition(Float boost,
                            List<Condition> must,
                            List<Condition> should,
                            List<Condition> not) {
        super(boost);
        this.must = must == null ? Collections.EMPTY_LIST : must;
        this.should = should == null ? Collections.EMPTY_LIST : should;
        this.not = not == null ? Collections.EMPTY_LIST : not;
    }

    /** {@inheritDoc} */
    @Override
    public BooleanQuery doQuery(Schema schema) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        must.forEach(condition -> builder.add(condition.query(schema), MUST));
        should.forEach(condition -> builder.add(condition.query(schema), SHOULD));
        not.forEach(condition -> builder.add(condition.query(schema), MUST_NOT));
        if (must.isEmpty() && should.isEmpty() && !not.isEmpty()) {
            logger.warn("Performing resource-intensive pure negation query {}", this);
            builder.add(new MatchAllDocsQuery(), FILTER);
        }
        return builder.build();
    }

    /** {@inheritDoc} */
    public Set<String> postProcessingFields() {
        Set<String> fields = new LinkedHashSet<>();
        must.forEach(condition -> fields.addAll(condition.postProcessingFields()));
        should.forEach(condition -> fields.addAll(condition.postProcessingFields()));
        return fields;
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("must", must).add("should", should).add("not", not);
    }
}
