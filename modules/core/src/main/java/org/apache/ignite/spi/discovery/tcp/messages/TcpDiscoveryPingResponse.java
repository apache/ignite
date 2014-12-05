/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.tcp.messages;

import java.io.*;
import java.util.*;

/**
 * Ping response.
 */
public class TcpDiscoveryPingResponse extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether pinged client exists. */
    private boolean clientExists;

    /**
     * For {@link Externalizable}.
     */
    public TcpDiscoveryPingResponse() {
        // No-op.
    }

    /**
     * @param creatorNodeId Creator node ID.
     */
    public TcpDiscoveryPingResponse(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /**
     * @param clientExists Whether pinged client exists.
     */
    public void clientExists(boolean clientExists) {
        this.clientExists = clientExists;
    }

    /**
     * @return Whether pinged client exists.
     */
    public boolean clientExists() {
        return clientExists;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(clientExists);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        clientExists = in.readBoolean();
    }
}
