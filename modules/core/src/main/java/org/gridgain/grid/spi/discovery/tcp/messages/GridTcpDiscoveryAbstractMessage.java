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
 * Base class to implement discovery messages.
 */
public abstract class GridTcpDiscoveryAbstractMessage implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    protected static final int CLIENT_FLAG_POS = 0;

    /** */
    protected static final int RESPONDED_FLAG_POS = 1;

    /** */
    protected static final int CLIENT_RECON_SUCCESS_FLAG_POS = 2;

    /** Sender of the message (transient). */
    private UUID senderNodeId;

    /** Message ID. */
    private IgniteUuid id;

    /** Verifier node ID. */
    private UUID verifierNodeId;

    /** Topology version. */
    private long topVer;

    /** Destination client node ID. */
    private UUID destClientNodeId;

    /** Flags. */
    @GridToStringExclude
    private int flags;

    /** Pending message index. */
    private short pendingIdx;

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
        id = IgniteUuid.fromUuid(creatorNodeId);
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
    public IgniteUuid id() {
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
        return getFlag(CLIENT_FLAG_POS);
    }

    /**
     * Sets client node flag.
     *
     * @param client Client node flag.
     */
    public void client(boolean client) {
        setFlag(CLIENT_FLAG_POS, client);
    }

    /**
     * @return Destination client node ID.
     */
    public UUID destinationClientNodeId() {
        return destClientNodeId;
    }

    /**
     * @param destClientNodeId Destination client node ID.
     */
    public void destinationClientNodeId(UUID destClientNodeId) {
        this.destClientNodeId = destClientNodeId;
    }

    /**
     * @return Pending message index.
     */
    public short pendingIndex() {
        return pendingIdx;
    }

    /**
     * @param pendingIdx Pending message index.
     */
    public void pendingIndex(short pendingIdx) {
        this.pendingIdx = pendingIdx;
    }

    /**
     * @param pos Flag position.
     * @return Flag value.
     */
    protected boolean getFlag(int pos) {
        assert pos >= 0 && pos < 32;

        int mask = 1 << pos;

        return (flags & mask) == mask;
    }

    /**
     * @param pos Flag position.
     * @param val Flag value.
     */
    protected void setFlag(int pos, boolean val) {
        assert pos >= 0 && pos < 32;

        int mask = 1 << pos;

        if (val)
            flags |= mask;
        else
            flags &= ~mask;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        U.writeUuid(out, verifierNodeId);
        out.writeLong(topVer);
        U.writeUuid(out, destClientNodeId);
        out.writeInt(flags);
        out.writeShort(pendingIdx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        verifierNodeId = U.readUuid(in);
        topVer = in.readLong();
        destClientNodeId = U.readUuid(in);
        flags = in.readInt();
        pendingIdx = in.readShort();
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
        return S.toString(GridTcpDiscoveryAbstractMessage.class, this, "isClient", getFlag(CLIENT_FLAG_POS));
    }
}
