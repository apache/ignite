// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.hadoop.*;

import java.util.*;

/**
 * NULL Counters implementation.
 */
public class GridHadoopEmptyCountersImpl implements GridHadoopCounters {
    /** */
    private static final GridHadoopEmptyCountersImpl INSTANCE = new GridHadoopEmptyCountersImpl();

    /**
     * Use {@link #instance()} method instead.
     */
    private GridHadoopEmptyCountersImpl() {
    }

    /**
     * @return Singleton instance.
     */
    public static GridHadoopEmptyCountersImpl instance() {
        return INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounter counter(String group, String name) {
        return new GridHadoopCounterImpl(group, name);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridHadoopCounter> all() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounters merge(GridHadoopCounters other) {
        return this;
    }
}
