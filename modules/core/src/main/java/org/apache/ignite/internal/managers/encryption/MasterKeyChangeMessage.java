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

package org.apache.ignite.internal.managers.encryption;

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Master key change message.
 * <p>
 * The master key change process consists of two phases:
 * <ul>
 *  <li>1. The initial phase is needed to check the consistency of the master key on all nodes. If an error occurs
 *  exception will be attached to the message and the process will be canceled.</li>
 *  <li>2. On the action phase, each node re-encrypts group keys and writes it to WAL and MetaStorage if possible.</li>
 * </ul>
 * <p>
 * For details, see {@code MasterKeyChangeListener}.
 */
public class MasterKeyChangeMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Request id. */
    private final UUID reqId;

    /** Encrypted master key id. */
    private final byte[] encKeyId;

    /** Master key digest. */
    private byte[] digest;

    /** Initial phase flag. */
    private final boolean isInit;

    /** Tuple of a node id and an error that caused this change to be rejected. */
    private T2<UUID, IgniteException> err;

    /**
     * Constructor for request.
     *
     * @param encKeyId Encrypted master key id.
     */
    public MasterKeyChangeMessage(byte[] encKeyId) {
        reqId = UUID.randomUUID();
        this.encKeyId = encKeyId;
        isInit = true;
    }

    /**
     * Constructor for response.
     *
     * @param req Request message.
     */
    private MasterKeyChangeMessage(MasterKeyChangeMessage req) {
        reqId = req.reqId;
        encKeyId = req.encKeyId;
        digest = req.digest;
        err = req.err;
        isInit = false;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoveryCustomMessage ackMessage() {
        return isInit() ? new MasterKeyChangeMessage(this) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** @return Request id. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Master key id. */
    public byte[] encKeyId() {
        return encKeyId;
    }

    /** @return Master key digest. */
    public byte[] digest() {
        return digest;
    }

    /** Sets master key digest. */
    public void digest(byte[] digest) {
        this.digest = digest;
    }

    /** @return Tuple of a node id and an error that caused this change to be rejected. */
    public T2<UUID, IgniteException> error() {
        return err;
    }

    /** @return {@code True} if error was reported during init. */
    public boolean hasError() {
        return err != null;
    }

    /**
     * @param nodeId Node id.
     * @param err Error caused this change to be rejected.
     */
    public void markRejected(UUID nodeId, IgniteException err) {
        if (!hasError())
            this.err = new T2<>(nodeId, err);
    }

    /**
     * Gets init flag.
     *
     * @return Init flag.
     */
    public boolean isInit() {
        return isInit;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MasterKeyChangeMessage.class, this);
    }
}
