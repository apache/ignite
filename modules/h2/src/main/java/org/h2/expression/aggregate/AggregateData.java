/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.value.Value;

/**
 * Abstract class for the computation of an aggregate.
 */
abstract class AggregateData {

    /**
     * Create an AggregateData object of the correct sub-type.
     *
     * @param aggregateType the type of the aggregate operation
     * @param distinct if the calculation should be distinct
     * @param dataType the data type of the computed result
     * @return the aggregate data object of the specified type
     */
    static AggregateData create(AggregateType aggregateType, boolean distinct, int dataType) {
        switch (aggregateType) {
        case COUNT_ALL:
            return new AggregateDataCount(true);
        case COUNT:
            if (!distinct) {
                return new AggregateDataCount(false);
            }
            break;
        case LISTAGG:
        case ARRAY_AGG:
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
        case CUME_DIST:
        case PERCENTILE_CONT:
        case PERCENTILE_DISC:
        case MEDIAN:
            break;
        case MIN:
        case MAX:
        case BIT_OR:
        case BIT_AND:
        case ANY:
        case EVERY:
            return new AggregateDataDefault(aggregateType, dataType);
        case SUM:
        case AVG:
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            if (!distinct) {
                return new AggregateDataDefault(aggregateType, dataType);
            }
            break;
        case SELECTIVITY:
            return new AggregateDataSelectivity(distinct);
        case HISTOGRAM:
            return new AggregateDataDistinctWithCounts(false, Constants.SELECTIVITY_DISTINCT_COUNT);
        case MODE:
            return new AggregateDataDistinctWithCounts(true, Integer.MAX_VALUE);
        case ENVELOPE:
            return new AggregateDataEnvelope();
        default:
            throw DbException.throwInternalError("type=" + aggregateType);
        }
        return new AggregateDataCollecting(distinct);
    }

    /**
     * Add a value to this aggregate.
     *
     * @param database the database
     * @param v the value
     */
    abstract void add(Database database, Value v);

    /**
     * Get the aggregate result.
     *
     * @param database the database
     * @param dataType the datatype of the computed result
     * @return the value
     */
    abstract Value getValue(Database database, int dataType);
}
