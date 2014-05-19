/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

/**
 * GGFS Hadoop stream descriptor.
 */
public class NewGridGgfsHadoopStreamDelegate {
    /** RPC handler. */
    private final NewGridGgfsHadoopEx hadoop;

    /** Target. */
    private final Object target;

    /** Optional stream length. */
    private final long len;

    /**
     * Constructor.
     *
     * @param target Target.
     */
    public NewGridGgfsHadoopStreamDelegate(NewGridGgfsHadoopEx hadoop, Object target) {
        this(hadoop, target, -1);
    }

    /**
     * Constructor.
     *
     * @param target Target.
     * @param len Optional length.
     */
    public NewGridGgfsHadoopStreamDelegate(NewGridGgfsHadoopEx hadoop, Object target, long len) {
        assert hadoop != null;
        assert target != null;

        this.hadoop = hadoop;
        this.target = target;
        this.len = len;
    }

    /**
     * @return RPC handler.
     */
    public NewGridGgfsHadoopEx hadoop() {
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
}
