// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.hadoop.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Adapter from own counter implementation into Hadoop API Counter od version 2.0.
 */
public class GridHadoopV2CounterAdapter implements Counter {

    /** Delegate. */
    private final GridHadoopCounter counter;

    /**
     * Creates new instance with given delegate.
     *
     * @param counter Internal counter.
     */
    public GridHadoopV2CounterAdapter(@NotNull GridHadoopCounter counter) {
        this.counter = counter;
    }

    /**
     * Returns underlying counter.
     *
     * @return Counter which performs all the job.
     */
    GridHadoopCounter delegate() {
        return counter;
    }

    /** {@inheritDoc} */
    @Override public void setDisplayName(String displayName) {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return counter.name();
    }

    /** {@inheritDoc} */
    @Override public String getDisplayName() {
        throw new UnsupportedOperationException("not implemented");
    }

    /** {@inheritDoc} */
    @Override public long getValue() {
        return counter.value();
    }

    /** {@inheritDoc} */
    @Override public void setValue(long value) {
        counter.value(value);
    }

    /** {@inheritDoc} */
    @Override public void increment(long incr) {
        counter.increment(incr);
    }

    /** {@inheritDoc} */
    @Override public Counter getUnderlyingCounter() {
        throw new UnsupportedOperationException("not implemented");
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
