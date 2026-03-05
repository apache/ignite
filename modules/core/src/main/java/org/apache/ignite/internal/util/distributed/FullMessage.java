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
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
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
    IgniteUuid id;

    /** Process id. */
    @Order(1)
    UUID processId;

    /** Process type. */
    @Order(2)
    int type;

    /** Results. */
    @Order(3)
    Map<UUID, R> res;

    /** Errors. */
    @Order(4)
    Map<UUID, ErrorMessage> err;

    /** Default constructor for {@link MessageFactory}. */
    public FullMessage() {
        // No-op.
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
        this.err = err == null ? null : F.viewReadOnly(err, ErrorMessage::new);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** @return Process id. */
    public UUID processId() {
        return processId;
    }

    /** @return Process type. */
    public int type() {
        return type;
    }

    /** @return Nodes results. */
    public Map<UUID, R> result() {
        return res;
    }

    /** @return Nodes errors. */
    public Map<UUID, Throwable> error() {
        return err == null ? null : F.viewReadOnly(err, e -> e.error());
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FullMessage.class, this);
    }
}
