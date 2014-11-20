/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Counter for the job statistics accumulation.
 */
public class GridHadoopStatCounter extends GridHadoopCounterAdapter<GridHadoopJobStatistics> {
    /** The group name for this counter. */
    public static final String GROUP_NAME = "SYSTEM";

    /** The counter name for this counter. */
    public static final String COUNTER_NAME = "STATISTICS";

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public GridHadoopStatCounter() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param grp Group name.
     * @param name Counter name.
     */
    public GridHadoopStatCounter(String grp, String name) {
        super(grp, name);

        value(new GridHadoopJobStatistics());
    }

    /** {@inheritDoc} */
    @Override protected void writeValue(ObjectOutput out) throws IOException {
        U.writeCollection(out, value().evts());
    }

    /** {@inheritDoc} */
    @Override protected void readValue(ObjectInput in) throws IOException {
        try {
            value(new GridHadoopJobStatistics(U.<T2<String, Long>>readCollection(in)));
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void append(GridHadoopJobStatistics val) {
        value().evts().addAll(val.evts());
    }
}
