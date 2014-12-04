/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Ping request.
 */
public class GridTcpDiscoveryPingRequest extends GridTcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Pinged client node ID. */
    private UUID clientNodeId;

    /**
     * For {@link Externalizable}.
     */
    public GridTcpDiscoveryPingRequest() {
        // No-op.
    }

    /**
     * @param creatorNodeId Creator node ID.
     * @param clientNodeId Pinged client node ID.
     */
    public GridTcpDiscoveryPingRequest(UUID creatorNodeId, @Nullable UUID clientNodeId) {
        super(creatorNodeId);

        this.clientNodeId = clientNodeId;
    }

    /**
     * @return Pinged client node ID.
     */
    @Nullable public UUID clientNodeId() {
        return clientNodeId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeUuid(out, clientNodeId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        clientNodeId = U.readUuid(in);
    }
}
