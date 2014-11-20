/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import java.io.*;

/**
 * Standard hadoop counter to use via original Hadoop API in Hadoop jobs.
 */
public class GridHadoopLongCounter extends GridHadoopCounterAdapter<Long> {
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

        value(0L);
    }

    /** {@inheritDoc} */
    @Override public void append(Long val) {
        value(value() + val);
    }

    /** {@inheritDoc} */
    @Override protected void writeValue(ObjectOutput out) throws IOException {
        out.writeLong(value());
    }

    /** {@inheritDoc} */
    @Override protected void readValue(ObjectInput in) throws IOException {
        value(in.readLong());
    }
}
