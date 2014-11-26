/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.hadoop.*;
import java.io.*;

/**
 * Standard hadoop counter to use via original Hadoop API in Hadoop jobs.
 */
public class GridHadoopLongCounter extends GridHadoopCounterAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** The counter value. */
    private long val;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public GridHadoopLongCounter() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param grp Group name.
     * @param name Counter name.
     */
    public GridHadoopLongCounter(String grp, String name) {
        super(grp, name);
    }

    /** {@inheritDoc} */
    @Override protected void writeValue(ObjectOutput out) throws IOException {
        out.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override protected void readValue(ObjectInput in) throws IOException {
        val = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public void merge(GridHadoopCounter cntr) {
        val += ((GridHadoopLongCounter)cntr).val;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounter copy() {
        GridHadoopLongCounter cp = new GridHadoopLongCounter(group(), name());

        cp.val = val;

        return cp;
    }

    /**
     * Gets current value of this counter.
     *
     * @return Current value.
     */
    public long value() {
        return val;
    }

    /**
     * Sets current value by the given value.
     *
     * @param val Value to set.
     */
    public void value(long val) {
        this.val = val;
    }

    /**
     * Increment this counter by the given value.
     *
     * @param i Value to increase this counter by.
     */
    public void increment(long i) {
        val += i;
    }
}
