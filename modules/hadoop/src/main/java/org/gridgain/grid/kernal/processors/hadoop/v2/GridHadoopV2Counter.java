/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.hadoop.*;

import java.io.*;

/**
 * Adapter from own counter implementation into Hadoop API Counter od version 2.0.
 */
public class GridHadoopV2Counter implements Counter {
    /** Delegate. */
    private final GridHadoopCounter<Long> cntr;

    /**
     * Creates new instance with given delegate.
     *
     * @param cntr Internal counter.
     */
    public GridHadoopV2Counter(GridHadoopCounter<Long> cntr) {
        assert cntr != null : "counter must be non-null";

        this.cntr = cntr;
    }

    /** {@inheritDoc} */
    @Override public void setDisplayName(String displayName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return cntr.name();
    }

    /** {@inheritDoc} */
    @Override public String getDisplayName() {
        return getName();
    }

    /** {@inheritDoc} */
    @Override public long getValue() {
        return cntr.value();
    }

    /** {@inheritDoc} */
    @Override public void setValue(long val) {
        cntr.value(val);
    }

    /** {@inheritDoc} */
    @Override public void increment(long incr) {
        cntr.append(incr);
    }

    /** {@inheritDoc} */
    @Override public Counter getUnderlyingCounter() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }
}
