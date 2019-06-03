/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.TreeMap;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate that needs distinct values with
 * their counts.
 */
class AggregateDataDistinctWithCounts extends AggregateData  {

    private final boolean ignoreNulls;

    private final int maxDistinctCount;

    private TreeMap<Value, LongDataCounter> values;

    private long allocated;

    /**
     * Creates new instance of data for aggregate that needs distinct values
     * with their counts.
     *
     * @param ignoreNulls
     *            whether NULL values should be ignored
     * @param maxDistinctCount
     *            maximum count of distinct values to collect
     */
    AggregateDataDistinctWithCounts(boolean ignoreNulls, int maxDistinctCount) {
        this.ignoreNulls = ignoreNulls;
        this.maxDistinctCount = maxDistinctCount;
    }

    @Override
    void add(Session ses, Value v) {
        if (ignoreNulls && v == ValueNull.INSTANCE) {
            return;
        }
        if (values == null) {
            values = new TreeMap<>(ses.getDatabase().getCompareMode());
        }
        LongDataCounter a = values.get(v);
        if (a == null) {
            if (values.size() >= maxDistinctCount) {
                return;
            }
            a = new LongDataCounter();
            values.put(v, a);

            H2MemoryTracker memTracker;
            if ((memTracker = ses.queryMemoryTracker()) != null) {
                long size = Constants.MEMORY_OBJECT;

                size += v.getMemory();

                memTracker.allocate(size);

                allocated += size;
            }
        }
        a.count++;
    }

    @Override
    Value getValue(Database database, int dataType) {
        return null;
    }

    /**
     * Returns map with values and their counts.
     *
     * @return map with values and their counts
     */
    TreeMap<Value, LongDataCounter> getValues() {
        return values;
    }

    /** {@inheritDoc} */
    @Override public void cleanup(Session ses) {
        H2MemoryTracker memTracker;
        if (values != null && (memTracker = ses.queryMemoryTracker()) != null) {
            values = null;

            memTracker.free(allocated);
        }
    }
}
