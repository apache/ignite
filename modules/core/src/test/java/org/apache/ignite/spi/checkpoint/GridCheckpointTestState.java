/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.checkpoint;

import java.io.*;

/**
 * Grid checkpoint test state
 */
public class GridCheckpointTestState implements Serializable {
    /** */
    private String data;

    /**
     * @param data Data.
     */
    public GridCheckpointTestState(String data) {
        this.data = data;
    }

    /**
     * Gets data.
     *
     * @return data.
     */
    public String getData() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return !(obj == null || !obj.getClass().equals(getClass())) && ((GridCheckpointTestState)obj).data.equals(data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return data.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getSimpleName());
        buf.append(" [data='").append(data).append('\'');
        buf.append(']');

        return buf.toString();
    }
}
