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
public class NewGridGgfsHadoopStreamDescriptor {
    /** Descriptor. */
    private final Object desc;

    /**
     * Constructor.
     *
     * @param desc Descriptor.
     */
    public NewGridGgfsHadoopStreamDescriptor(Object desc) {
        this.desc = desc;
    }

    /**
     * @return Entity describing the stream.
     */
    @SuppressWarnings("unchecked")
    public <T> T get() {
        return (T)desc;
    }
}
