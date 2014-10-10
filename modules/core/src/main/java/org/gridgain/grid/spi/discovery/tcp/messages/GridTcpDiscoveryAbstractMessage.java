/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Base class to implement discovery messages.
 */
public abstract class GridTcpDiscoveryAbstractMessage implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Sender of the message (transient). */
    private UUID senderNodeId;

    /** Message ID. */
    private GridUuid id;

    /** Verifier node ID. */
    private UUID verifierNodeId;

    /** Topology version. */
    private long topVer;

    /** Client node flag. */
    private boolean client;

    /** Whether to redirect to client nodes. */
    private boolean redirectToClients = true;

    /**
     * Default no-arg constructor for {@link Externalizable} interface.
     */
    protected GridTcpDiscoveryAbstractMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     */
    protected GridTcpDiscoveryAbstractMessage(UUID creatorNodeId) {
        id = GridUuid.fromUuid(creatorNodeId);
    }

    /**
     * Gets creator node.
     *
     * @return Creator node ID.
     */
    public UUID creatorNodeId() {
        return id.globalId();
    }

    /**
     * Gets message ID.
     *
     * @return Message ID.
     */
    public GridUuid id() {
        return id;
    }

    /**
     * Gets sender node ID.
     *
     * @return Sender node ID.
     */
    public UUID senderNodeId() {
        return senderNodeId;
    }

    /**
     * Sets sender node ID.
     *
     * @param senderNodeId Sender node ID.
     */
    public void senderNodeId(UUID senderNodeId) {
        this.senderNodeId = senderNodeId;
    }

    /**
     * Checks whether message is verified.
     *
     * @return {@code true} if message was verified.
     */
    public boolean verified() {
        return verifierNodeId != null;
    }

    /**
     * Gets verifier node ID.
     *
     * @return verifier node ID.
     */
    public UUID verifierNodeId() {
        return verifierNodeId;
    }

    /**
     * Verifies the message and stores verifier ID.
     *
     * @param verifierNodeId Verifier node ID.
     */
    public void verify(UUID verifierNodeId) {
        this.verifierNodeId = verifierNodeId;
    }

    /**
     * Gets topology version.
     *
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * Sets topology version.
     *
     * @param topVer Topology version.
     */
    public void topologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     * Get client node flag.
     *
     * @return Client node flag.
     */
    public boolean client() {
        return client;
    }

    /**
     * Sets client node flag.
     *
     * @param client Client node flag.
     */
    public void client(boolean client) {
        this.client = client;
    }

    /**
     * @return Whether to redirect to clients.
     */
    public boolean redirectToClients() {
        return redirectToClients;
    }

    /**
     * @param redirectToClients Whether to redirect to clients.
     */
    public void redirectToClients(boolean redirectToClients) {
        this.redirectToClients = redirectToClients;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        U.writeUuid(out, verifierNodeId);
        out.writeLong(topVer);
        out.writeBoolean(client);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        verifierNodeId = U.readUuid(in);
        topVer = in.readLong();
        client = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public final boolean equals(Object obj) {
        if (this == obj)
            return true;
        else if (obj instanceof GridTcpDiscoveryAbstractMessage)
            return id.equals(((GridTcpDiscoveryAbstractMessage)obj).id);

        return false;
    }

    /** {@inheritDoc} */
    @Override public final int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryAbstractMessage.class, this);
    }
}
