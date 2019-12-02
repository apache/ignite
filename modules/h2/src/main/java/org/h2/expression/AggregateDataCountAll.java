/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating a COUNT(*) aggregate.
 */
class AggregateDataCountAll extends AggregateData {
    private long count;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (distinct) {
            throw DbException.throwInternalError();
        }
        count++;
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            throw DbException.throwInternalError();
        }
        Value v = ValueLong.get(count);
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }

}
