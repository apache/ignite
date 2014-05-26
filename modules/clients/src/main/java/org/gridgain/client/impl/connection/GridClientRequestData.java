/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl.connection;

import java.util.*;

/**
 * Client request data.
 */
class GridClientRequestData {
    /** Request Id. */
    private final long reqId;

    /** Request Id. */
    private final UUID clientId;

    /** Request Id. */
    private final UUID destId;

    /** Request body. */
    private byte[] body;

    /**
     * @param reqId Request Id.
     * @param clientId Client Id.
     * @param destId Destination Id.
     */
    GridClientRequestData(long reqId, UUID clientId, UUID destId) {
        this.reqId = reqId;
        this.clientId = clientId;
        this.destId = destId;
    }

    /**
     * @return Request Id.
     */
    long requestId() {
        return reqId;
    }

    /**
     * @return Client Id.
     */
    UUID clientId() {
        return clientId;
    }

    /**
     * @return Destination Id.
     */
    UUID destinationId() {
        return destId;
    }

    /**
     * @param body Request body.
     */
    void body(byte[] body) {
        this.body = body;
    }

    /**
     * @return Request body.
     */
    byte[] body() {
        return body;
    }
}
