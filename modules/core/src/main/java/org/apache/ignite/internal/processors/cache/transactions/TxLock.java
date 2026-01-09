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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Corresponds to one {@link GridCacheMvccCandidate} from local MVCC candidates queue.
 * There is one exclusion: {@link TxLock} instance with {@link #OWNERSHIP_REQUESTED} corresponds to lock request
 * to remote node from near node that isn't primary node for key.
 */
public class TxLock implements Message {
    /** Ownership owner. */
    static final byte OWNERSHIP_OWNER = 1;

    /** Ownership candidate. */
    static final byte OWNERSHIP_CANDIDATE = 2;

    /** Ownership requested. */
    static final byte OWNERSHIP_REQUESTED = 3;

    /** Near node ID. */
    @Order(0)
    private UUID nearNodeId;

    /** Tx ID. */
    @Order(1)
    private GridCacheVersion txId;

    /** Thread ID. */
    @Order(2)
    private long threadId;

    /** Ownership. */
    @Order(3)
    private byte ownership;

    /**
     * Default constructor.
     */
    public TxLock() {
        // No-op.
    }

    /**
     * @param txId Tx ID.
     * @param nearNodeId Near node ID.
     * @param threadId Thread ID.
     * @param ownership Ownership.
     */
    public TxLock(GridCacheVersion txId, UUID nearNodeId, long threadId, byte ownership) {
        this.txId = txId;
        this.nearNodeId = nearNodeId;
        this.threadId = threadId;
        this.ownership = ownership;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @param nearNodeId  Near node ID.
     */
    public void nearNodeId(UUID nearNodeId) {
        this.nearNodeId = nearNodeId;
    }

    /**
     * @return Transaction ID.
     */
    public GridCacheVersion txId() {
        return txId;
    }

    /**
     * @param txId  Transaction ID.
     */
    public void txId(GridCacheVersion txId) {
        this.txId = txId;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @param threadId  Thread ID.
     */
    public void threadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * @return Ownership.
     */
    public byte ownership() {
        return ownership;
    }

    /**
     * @param ownership Ownership.
     */
    public void ownership(byte ownership) {
        this.ownership = ownership;
    }

    /**
     * @return {@code True} if transaction hold lock on the key, otherwise {@code false}.
     */
    public boolean owner() {
        return ownership == OWNERSHIP_OWNER;
    }

    /**
     * @return {@code True} if there is MVCC candidate for this transaction and key, otherwise {@code false}.
     */
    public boolean candiate() {
        return ownership == OWNERSHIP_CANDIDATE;
    }

    /**
     * @return {@code True} if transaction requested lock for key from primary remote node
     * but response isn't received because other transaction hold lock on the key.
     */
    public boolean requested() {
        return ownership == OWNERSHIP_REQUESTED;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLock.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -25;
    }

}
