/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Message telling that client node is reconnecting to topology.
 */
public class GridTcpDiscoveryClientReconnectMessage extends GridTcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** New router nodeID. */
    private UUID routerNodeId;

    /** Last message ID. */
    private GridUuid lastMsgId;

    /** Pending messages. */
    @GridToStringExclude
    private Collection<GridTcpDiscoveryAbstractMessage> msgs;

    /**
     * For {@link Externalizable}.
     */
    public GridTcpDiscoveryClientReconnectMessage() {
        // No-op.
    }

    /**
     * @param creatorNodeId Creator node ID.
     * @param routerNodeId New router node ID.
     * @param lastMsgId Last message ID.
     */
    public GridTcpDiscoveryClientReconnectMessage(UUID creatorNodeId, UUID routerNodeId, GridUuid lastMsgId) {
        super(creatorNodeId);

        this.routerNodeId = routerNodeId;
        this.lastMsgId = lastMsgId;
    }

    /**
     * @return New router node ID.
     */
    public UUID routerNodeId() {
        return routerNodeId;
    }

    /**
     * @return Last message ID.
     */
    public GridUuid lastMessageId() {
        return lastMsgId;
    }

    /**
     * @param msgs Pending messages.
     */
    public void pendingMessages(Collection<GridTcpDiscoveryAbstractMessage> msgs) {
        this.msgs = msgs;
    }

    /**
     * @return Pending messages.
     */
    public Collection<GridTcpDiscoveryAbstractMessage> pendingMessages() {
        return msgs;
    }

    /**
     * @param success Success flag.
     */
    public void success(boolean success) {
        setFlag(CLIENT_RECON_SUCCESS_FLAG_POS, success);
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return getFlag(CLIENT_RECON_SUCCESS_FLAG_POS);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeUuid(out, routerNodeId);
        U.writeGridUuid(out, lastMsgId);
        U.writeCollection(out, msgs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        routerNodeId = U.readUuid(in);
        lastMsgId = U.readGridUuid(in);
        msgs = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryClientReconnectMessage.class, this, "super", super.toString());
    }
}
