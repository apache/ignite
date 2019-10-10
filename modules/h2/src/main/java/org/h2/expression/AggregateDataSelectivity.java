/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.util.IntIntHashMap;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * Data stored while calculating a SELECTIVITY aggregate.
 */
class AggregateDataSelectivity extends AggregateData {
    private long count;
    private IntIntHashMap distinctHashes;
    private double m2;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        count++;
        if (distinctHashes == null) {
            distinctHashes = new IntIntHashMap();
        }
        int size = distinctHashes.size();
        if (size > Constants.SELECTIVITY_DISTINCT_COUNT) {
            distinctHashes = new IntIntHashMap();
            m2 += size;
        }
        int hash = v.hashCode();
        // the value -1 is not supported
        distinctHashes.put(hash, 1);
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            count = 0;
        }
        Value v = null;
        int s = 0;
        if (count == 0) {
            s = 0;
        } else {
            m2 += distinctHashes.size();
            m2 = 100 * m2 / count;
            s = (int) m2;
            s = s <= 0 ? 1 : s > 100 ? 100 : s;
        }
        v = ValueInt.get(s);
        return v.convertTo(dataType);
    }
}
