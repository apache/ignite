/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.*;
import org.gridgain.grid.hadoop.*;

/**
 * Hadoop reporter implementation for v1 API.
 */
public class GridHadoopV1Reporter implements Reporter {
    /** Context. */
    private final GridHadoopTaskContext ctx;

    /**
     * Creates new instance.
     *
     * @param ctx Context.
     */
    public GridHadoopV1Reporter(GridHadoopTaskContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void setStatus(String status) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public Counters.Counter getCounter(Enum<?> name) {
        return getCounter(name.getDeclaringClass().getName(), name.name());
    }

    /** {@inheritDoc} */
    @Override public Counters.Counter getCounter(String group, String name) {
        return new GridHadoopV1Counter(ctx.counter(group, name));
    }

    /** {@inheritDoc} */
    @Override public void incrCounter(Enum<?> key, long amount) {
        getCounter(key).increment(amount);
    }

    /** {@inheritDoc} */
    @Override public void incrCounter(String group, String counter, long amount) {
        getCounter(group, counter).increment(amount);
    }

    /** {@inheritDoc} */
    @Override public InputSplit getInputSplit() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("reporter has no input"); // TODO
    }

    /** {@inheritDoc} */
    @Override public float getProgress() {
        return 0.5f; // TODO
    }

    /** {@inheritDoc} */
    @Override public void progress() {
        // TODO
    }
}
