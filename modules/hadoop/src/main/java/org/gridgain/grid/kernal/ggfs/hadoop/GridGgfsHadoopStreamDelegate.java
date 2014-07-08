/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * GGFS Hadoop stream descriptor.
 */
public class GridGgfsHadoopStreamDelegate {
    /** RPC handler. */
    private final GridGgfsHadoopEx hadoop;

    /** Target. */
    private final Object target;

    /** Optional stream length. */
    private final long len;

    /**
     * Constructor.
     *
     * @param target Target.
     */
    public GridGgfsHadoopStreamDelegate(GridGgfsHadoopEx hadoop, Object target) {
        this(hadoop, target, -1);
    }

    /**
     * Constructor.
     *
     * @param target Target.
     * @param len Optional length.
     */
    public GridGgfsHadoopStreamDelegate(GridGgfsHadoopEx hadoop, Object target, long len) {
        assert hadoop != null;
        assert target != null;

        this.hadoop = hadoop;
        this.target = target;
        this.len = len;
    }

    /**
     * @return RPC handler.
     */
    public GridGgfsHadoopEx hadoop() {
        return hadoop;
    }

    /**
     * @return Stream target.
     */
    @SuppressWarnings("unchecked")
    public <T> T target() {
        return (T) target;
    }

    /**
     * @return Length.
     */
    public long length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return System.identityHashCode(target);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj != null && obj instanceof GridGgfsHadoopStreamDelegate &&
            target == ((GridGgfsHadoopStreamDelegate)obj).target;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsHadoopStreamDelegate.class, this);
    }
}
