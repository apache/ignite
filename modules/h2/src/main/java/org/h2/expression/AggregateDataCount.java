/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Database;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataCount extends AggregateData {
    private long count;
    private ValueHashMap<AggregateDataCount> distinctValues;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        count++;
        if (distinct) {
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
            }
            distinctValues.put(v, this);
        }
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            if (distinctValues != null) {
                count = distinctValues.size();
            } else {
                count = 0;
            }
        }
        Value v = ValueLong.get(count);
        return v.convertTo(dataType);
    }

}
