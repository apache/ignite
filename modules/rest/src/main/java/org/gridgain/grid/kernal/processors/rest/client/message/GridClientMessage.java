/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.rest.client.message;

import java.io.*;
import java.util.*;

/**
 * Interface for all client messages.
 */
public interface GridClientMessage extends Serializable {
    /**
     * This method is used to match request and response messages.
     *
     * @return request ID.
     */
    public long requestId();

    /**
     * Sets request id for outgoing packets.
     *
     * @param reqId request ID.
     */
    public void requestId(long reqId);

    /**
     * Gets client identifier from which this request comes.
     *
     * @return Client identifier.
     */
    public UUID clientId();

    /**
     * Sets client identifier from which this request comes.
     *
     * @param id Client identifier.
     */
    public void clientId(UUID id);

    /**
     * Gets identifier of the node where this message should be processed.
     *
     * @return Client identifier.
     */
    public UUID destinationId();

    /**
     * Sets identifier of the node where this message should be eventually delivered.
     *
     * @param id Client identifier.
     */
    public void destinationId(UUID id);

    /**
     * Gets client session token.
     *
     * @return Session token.
     */
    public byte[] sessionToken();

    /**
     * Sets client session token.
     *
     * @param sesTok Session token.
     */
    public void sessionToken(byte[] sesTok);
}
