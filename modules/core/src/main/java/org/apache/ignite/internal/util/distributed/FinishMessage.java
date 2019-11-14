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

package org.apache.ignite.internal.util.distributed;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Finish process message.
 */
public class FinishMessage implements DiscoveryCustomMessage {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Requiest id. */
    private final UUID reqId;

    /** Error. */
    private Exception err;

    /** Result. */
    @GridToStringInclude
    private Serializable res;

    /**
     * @param reqId Requiest id.
     * @param res Result.
     */
    public FinishMessage(UUID reqId, Serializable res) {
        this.reqId = reqId;
        this.res = res;
    }

    /**
     * @param reqId Requiest id.
     * @param err Error.
     */
    public FinishMessage(UUID reqId, Exception err) {
        this.reqId = reqId;
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return null;
    }

    /** @return Requiest id. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Result. */
    public Serializable result() {
        return res;
    }

    /** @return {@code True} if process finished with error. */
    public boolean hasError() {
        return err != null;
    }

    /** @return Error. */
    public Exception error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FinishMessage.class, this);
    }
}
