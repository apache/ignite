/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;

/**
 *
 */
public interface GridHadoopCommunicationClient {
    /**
     * @return {@code True} if client has been closed by this call,
     *      {@code false} if failed to close client (due to concurrent reservation or concurrent close).
     */
    public boolean close();

    /**
     * Forces client close.
     */
    public void forceClose();

    /**
     * @return {@code True} if client is closed;
     */
    public boolean closed();

    /**
     * @return {@code True} if client was reserved, {@code false} otherwise.
     */
    public boolean reserve();

    /**
     * Releases this client by decreasing reservations.
     */
    public void release();

    /**
     * @return {@code True} if client was reserved.
     */
    public boolean reserved();

    /**
     * Gets idle time of this client.
     *
     * @return Idle time of this client.
     */
    public long getIdleTime();

    /**
     * @param desc Process descriptor.
     * @param msg Message to send.
     * @throws GridException If failed.
     */
    public void sendMessage(GridHadoopProcessDescriptor desc, GridHadoopMessage msg) throws GridException;
}
