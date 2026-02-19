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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Full process message. Contains single nodes results.
 *
 * @param <R> Result type.
 * @see DistributedProcess
 * @see InitMessage
 * @see SingleNodeMessage
 */
public class FullMessage<R extends Message> implements DiscoveryCustomMessage, Message {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    @Order(0)
    private IgniteUuid id;

    /** Process id. */
    @Order(1)
    private UUID processId;

    /** Process type. */
    @Order(2)
    private int type;

    /** Results. */
    @Order(value = 3, method = "result")
    private Map<UUID, R> res;

    /** Errors. */
    private Map<UUID, Throwable> err;

    /** */
    @Order(value = 4, method = "errorMessages")
    private Map<UUID, ErrorMessage> errMsgs;

    /** */
    public FullMessage() {

    }

    /**
     * @param processId Process id.
     * @param type Process type.
     * @param res Results.
     * @param err Errors
     */
    public FullMessage(UUID processId, DistributedProcessType type, Map<UUID, R> res, Map<UUID, Throwable> err) {
        this.id = IgniteUuid.randomUuid();
        this.processId = processId;
        this.type = type.ordinal();
        this.res = res;
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** */
    public void id(IgniteUuid id) {
        this.id = id;
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
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return null;
    }

    /** @return Process id. */
    public UUID processId() {
        return processId;
    }

    /** */
    public void processId(UUID processId) {
        this.processId = processId;
    }

    /** @return Process type. */
    public int type() {
        return type;
    }

    /** */
    public void type(int type) {
        this.type = type;
    }

    /** @return Nodes results. */
    public Map<UUID, R> result() {
        return res;
    }

    /** */
    public void result(Map<UUID, R> res) {
        this.res = res;
    }

    /** @return Nodes errors. */
    public Map<UUID, Throwable> error() {
        return err;
    }

    /** @return Error messages map. */
    public Map<UUID, ErrorMessage> errorMessages() {
        return err == null ? null : F.viewReadOnly(err, ErrorMessage::new);
    }

    /** @param errMsgs Error messages map. */
    public void errorMessages(Map<UUID, ErrorMessage> errMsgs) {
        err = errMsgs == null ? null : F.viewReadOnly(errMsgs, e -> e.error());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FullMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 22;
    }
}
