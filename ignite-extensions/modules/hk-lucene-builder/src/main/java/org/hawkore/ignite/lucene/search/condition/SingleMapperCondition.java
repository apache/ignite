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
import org.apache.lucene.search.Query;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.Schema;
import org.hawkore.ignite.lucene.schema.mapping.Mapper;

/**
 * An abstract {@link Condition} using a specific {@link Mapper}.
 *
 * Known subclasses are: <ul> <li> {@link BitemporalCondition} <li> {@link DateRangeCondition} <li> {@link
 * GeoDistanceCondition} <li> {@link GeoBBoxCondition} </ul>
 *
 * @param <T> The specific {@link Mapper} type.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleMapperCondition<T extends Mapper> extends SingleFieldCondition {

    /** The type of the {@link Mapper}. */
    protected final Class<? extends T> type;

    /**
     * Constructor using the boost and the name of the mapper.
     *
     * @param boost the boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param type the type of the {@link Mapper}
     */
    protected SingleMapperCondition(Float boost, String field, Class<? extends T> type) {
        super(boost, field);
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public final Query doQuery(Schema schema) {
        Mapper mapper = schema.mapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for field '{}'", field);
        } else if (!type.isAssignableFrom(mapper.getClass())) {
            throw new IndexException("Field '{}' requires a mapper of type '{}' but found '{}'", field, type, mapper);
        }
        return doQuery((T) mapper, schema.analyzer);
    }

    /**
     * Returns the Lucene {@link Query} representation of this condition.
     *
     * @param mapper The {@link Mapper} to be used.
     * @param analyzer The {@link Schema} {@link Analyzer}.
     * @return The Lucene {@link Query} representation of this condition.
     */
    @SuppressWarnings("UnusedParameters")
    public abstract Query doQuery(T mapper, Analyzer analyzer);
}