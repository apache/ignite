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

import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.tree.NumberRangePrefixTree.NRShape;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.mapping.DateRangeMapper;

import com.google.common.base.MoreObjects;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeCondition extends SingleMapperCondition<DateRangeMapper> {

    /** The default from value. */
    public static final Date DEFAULT_FROM = new Date(Long.MIN_VALUE);

    /** The default to value. */
    public static final Date DEFAULT_TO = new Date(Long.MAX_VALUE);

    /** The default operation. */
    public static final String DEFAULT_OPERATION = "intersects";

    /** The lower accepted value. Maybe null meaning no lower limit. */
    public final Object from;

    /** The upper accepted value. Maybe null meaning no upper limit. */
    public final Object to;

    /** The spatial operation to be performed. */
    public final String operation;

    /**
     * Constructs a query selecting all fields greater/equal than {@code from} but less/equal than {@code to}.
     *
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     *
     * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
     * weightings) have their score multiplied by {@code boost}.
     * @param field the name of the field to be matched
     * @param from the lower accepted {@link Date}. Maybe {@code null} meaning no lower limit
     * @param to the upper accepted {@link Date}. Maybe {@code null} meaning no upper limit
     * @param operation the spatial operation to be performed
     */
    public DateRangeCondition(Float boost, String field, Object from, Object to, String operation) {
        super(boost, field, DateRangeMapper.class);
        this.from = from == null ? DEFAULT_FROM : from;
        this.to = to == null ? DEFAULT_TO : to;
        this.operation = operation == null ? DEFAULT_OPERATION : operation;
    }

    /** {@inheritDoc} */
    @Override
    public Query doQuery(DateRangeMapper mapper, Analyzer analyzer) {

        SpatialStrategy strategy = mapper.strategy;

        Date fromDate = mapper.base(from);
        Date toDate = mapper.base(to);

        NRShape shape = mapper.makeShape(fromDate, toDate);

        SpatialOperation spatialOperation = parseSpatialOperation(operation);

        SpatialArgs args = new SpatialArgs(spatialOperation, shape);
        return strategy.makeQuery(args);
    }

    /**
     * Returns the {@link SpatialOperation} representing the specified {@code String}.
     *
     * @param operation a {@code String} representing a {@link SpatialOperation}
     * @return the {@link SpatialOperation} representing the specified {@code String}
     */
    static SpatialOperation parseSpatialOperation(String operation) {
        if (operation == null) {
            throw new IndexException("Operation is required");
        } else if (operation.equalsIgnoreCase("is_within")) {
            return SpatialOperation.IsWithin;
        } else if (operation.equalsIgnoreCase("contains")) {
            return SpatialOperation.Contains;
        } else if (operation.equalsIgnoreCase("intersects")) {
            return SpatialOperation.Intersects;
        } else {
            throw new IndexException("Operation is invalid: {}", operation);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(this).add("from", from).add("to", to).add("operation", operation);
    }
}