/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.communication;

import org.gridgain.grid.lang.*;

import java.io.*;
import java.util.*;

/**
 * Listener SPI notifies IO manager with.
 * <p>
 * {@link GridCommunicationSpi} should ignore very first 4 bytes received from
 * sender node and pass the rest of the message to the listener.
 */
public interface GridCommunicationListener<T extends Serializable> {
    /**
     * <b>NOTE:</b> {@link GridCommunicationSpi} should ignore very first 4 bytes received from
     * sender node and pass the rest of the received message to the listener.
     *
     * @param nodeId Node ID.
     * @param msg Message.
     * @param msgC Runnable to call when message processing finished.
     */
    public void onMessage(UUID nodeId, T msg, IgniteRunnable msgC);

    /**
     * Callback invoked when connection with remote node is lost.
     *
     * @param nodeId Node ID.
     */
    public void onDisconnected(UUID nodeId);
}
