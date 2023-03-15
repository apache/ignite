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

import java.util.Collections;
import java.util.Set;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.hawkore.ignite.lucene.schema.Schema;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches none documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class NoneCondition extends Condition {

    /**
     * Constructor without field arguments.
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     */
    public NoneCondition(Float boost) {
        super(boost);
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(Schema schema) {
        return new BooleanQuery.Builder().build();
    }

    /** {@inheritDoc} */
    public Set<String> postProcessingFields() {
        return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this);
    }
}