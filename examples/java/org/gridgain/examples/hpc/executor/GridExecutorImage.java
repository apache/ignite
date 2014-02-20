// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.executor;

import java.io.*;

/**
 * Contain image result data.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridExecutorImage implements Serializable {
    /** Image height. */
    private int height;

    /** Image width. */
    private int width;

    /** Image binary data. */
    private byte[] data;

    /**
     * Creates image.
     *
     * @param height Image height.
     * @param width Image width.
     * @param data Image binary data.
     */
    public GridExecutorImage(int height, int width, byte[] data) {
        assert data != null;

        this.height = height;
        this.width = width;
        this.data = data;
    }

    /**
     * Gets height.
     *
     * @return Image height.
     */
    public int getHeight() {
        return height;
    }

    /**
     * Gets width.
     *
     * @return Image width.
     */
    public int getWidth() {
        return width;
    }

    /**
     * Gets image binary data.
     *
     * @return Image binary data.
     */
    public byte[] getData() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getName());
        buf.append(" [height=").append(height);
        buf.append(", width=").append(width);
        buf.append(']');

        return buf.toString();
    }
}
