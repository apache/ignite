/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.spi.discovery.tcp.internal.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Message sent by node to its next to ensure that next node and
 * connection to it are alive. Receiving node should send it across the ring,
 * until message does not reach coordinator. Coordinator responds directly to node.
 * <p>
 * If a failed node id is specified then the message is sent across the ring up to the sender node
 * to ensure that the failed node is actually failed.
 */
public class TcpDiscoveryStatusCheckMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Status OK. */
    public static final int STATUS_OK = 1;

    /** Status RECONNECT. */
    public static final int STATUS_RECON = 2;

    /** Creator node. */
    private TcpDiscoveryNode creatorNode;

    /** Failed node id. */
    private UUID failedNodeId;

    /** Creator node status (initialized by coordinator). */
    private int status;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryStatusCheckMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNode Creator node.
     * @param failedNodeId Failed node id.
     */
    public TcpDiscoveryStatusCheckMessage(TcpDiscoveryNode creatorNode, UUID failedNodeId) {
        super(creatorNode.id());

        this.creatorNode = creatorNode;
        this.failedNodeId = failedNodeId;
    }

    /**
     * Gets creator node.
     *
     * @return Creator node.
     */
    public TcpDiscoveryNode creatorNode() {
        return creatorNode;
    }

    /**
     * Gets failed node id.
     *
     * @return Failed node id.
     */
    public UUID failedNodeId() {
        return failedNodeId;
    }

    /**
     * Gets creator status.
     *
     * @return Creator node status.
     */
    public int status() {
        return status;
    }

    /**
     * Sets creator node status (should be set by coordinator).
     *
     * @param status Creator node status.
     */
    public void status(int status) {
        this.status = status;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(creatorNode);
        U.writeUuid(out, failedNodeId);
        out.writeInt(status);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        creatorNode = (TcpDiscoveryNode)in.readObject();
        failedNodeId = U.readUuid(in);
        status = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryStatusCheckMessage.class, this, "super", super.toString());
    }
}
