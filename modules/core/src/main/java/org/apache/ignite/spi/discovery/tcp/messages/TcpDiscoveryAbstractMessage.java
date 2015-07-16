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

import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.util.*;

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
    protected static final int CLIENT_ACK_FLAG_POS = 4;

    /** Sender of the message (transient). */
    private transient UUID sndNodeId;

    /** Message ID. */
    private IgniteUuid id;

    /** Verifier node ID. */
    private UUID verifierNodeId;

    /** Topology version. */
    private long topVer;

    /** Flags. */
    @GridToStringExclude
    private int flags;

    /** Pending message index. */
    private short pendingIdx;

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

    /**
     * @return {@code true} if message must be added to head of queue.
     */
    public boolean highPriority() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public final boolean equals(Object obj) {
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
