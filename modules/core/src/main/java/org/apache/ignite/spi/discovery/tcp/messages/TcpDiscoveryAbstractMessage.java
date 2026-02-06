/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp.messages;

import java.io.Externalizable;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Base class to implement discovery messages.
 */
public abstract class TcpDiscoveryAbstractMessage implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    protected static final int CLIENT_FLAG_POS = 0;

    /** */
    protected static final int RESPONDED_FLAG_POS = 1;

    /** */
    protected static final int CLIENT_RECON_SUCCESS_FLAG_POS = 2;

    /** */
    protected static final int FORCE_FAIL_FLAG_POS = 4;

    /** Sender of the message (transient). */
    private transient UUID sndNodeId;

    /** Message ID. */
    @Order(0)
    private IgniteUuid id;

    /**
     * Verifier node ID.
     * Node can mark the messages as verified for rest of nodes to apply the
     * changes this message is issued for, i.e. node added message, node failed or
     * left message are processed by other nodes only after coordinator
     * verification.
     */
    @Order(1)
    private UUID verifierNodeId;

    /** Topology version. */
    @Order(value = 2, method = "topologyVersion")
    private long topVer;

    /** Flags. */
    @GridToStringExclude
    @Order(3)
    private int flags;

    /** */
    @GridToStringInclude
    @Order(4)
    private Set<UUID> failedNodes;

    /** Helps to avoid multiply serialization in case message is sended more than once using different workers. */
    public byte[] serializedData() {
        return serData;
    }

    /** */
    public void serializedData(byte[] serData) {
        this.serData = serData;
    }

    /** */
    private byte[] serData;

    /**
     * Default no-arg constructor for {@link Externalizable} interface.
     */
    protected TcpDiscoveryAbstractMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     */
    protected TcpDiscoveryAbstractMessage(UUID creatorNodeId) {
        id = IgniteUuid.fromUuid(creatorNodeId);
    }

    /**
     * @param msg Message.
     */
    protected TcpDiscoveryAbstractMessage(TcpDiscoveryAbstractMessage msg) {
        id = msg.id;
        verifierNodeId = msg.verifierNodeId;
        topVer = msg.topVer;
        flags = msg.flags;
    }

    /**
     * @return {@code True} if need use trace logging for this message (to reduce amount of logging with debug level).
     */
    public boolean traceLogLevel() {
        return false;
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
     * Sets message ID.
     *
     * @param id Message ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /**
     * Gets sender node ID.
     *
     * @return Sender node ID.
     */
    public UUID senderNodeId() {
        return sndNodeId;
    }

    /**
     * Sets sender node ID.
     *
     * @param sndNodeId Sender node ID.
     */
    public void senderNodeId(UUID sndNodeId) {
        this.sndNodeId = sndNodeId;
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
    public void verifierNodeId(UUID verifierNodeId) {
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
     * Get force fail node flag.
     *
     * @return Force fail node flag.
     */
    public boolean force() {
        return getFlag(FORCE_FAIL_FLAG_POS);
    }

    /**
     * Sets force fail node flag.
     *
     * @param force Force fail node flag.
     */
    public void force(boolean force) {
        setFlag(FORCE_FAIL_FLAG_POS, force);
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

    /**
     * @return {@code true} if message must be added to head of queue.
     */
    public boolean highPriority() {
        return false;
    }

    /**
     * Adds node ID to the failed nodes list.
     *
     * @param nodeId Node ID.
     */
    public void addFailedNode(UUID nodeId) {
        assert nodeId != null;

        if (failedNodes == null)
            failedNodes = new HashSet<>();

        failedNodes.add(nodeId);
    }

    /**
     * @param failedNodes Failed nodes.
     */
    public void failedNodes(@Nullable Set<UUID> failedNodes) {
        this.failedNodes = failedNodes;
    }

    /**
     * @return Failed nodes IDs.
     */
    @Nullable public Set<UUID> failedNodes() {
        return failedNodes;
    }

    /**
     * @return Flags.
     */
    public int flags() {
        return flags;
    }

    /**
     * @param flags New flags.
     */
    public void flags(int flags) {
        this.flags = flags;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;
        else if (obj instanceof TcpDiscoveryAbstractMessage)
            return id.equals(((TcpDiscoveryAbstractMessage)obj).id);

        return false;
    }

    /** {@inheritDoc} */
    @Override public final int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryAbstractMessage.class, this, "isClient", getFlag(CLIENT_FLAG_POS));
    }
}
