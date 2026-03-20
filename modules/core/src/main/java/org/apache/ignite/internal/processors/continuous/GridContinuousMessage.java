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

package org.apache.ignite.internal.processors.continuous;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.continuous.GridContinuousMessageType.MSG_EVT_ACK;

/**
 * Continuous processor message.
 */
public class GridContinuousMessage implements Message {
    /** Message type. */
    @Order(0)
    GridContinuousMessageType type;

    /** Routine ID. */
    @Order(1)
    UUID routineId;

    /** Optional message data. */
    @GridToStringInclude(sensitive = true)
    private Object data;

    /** */
    @Order(2)
    Collection<Message> msgs;

    /** Serialized message data. */
    @Order(3)
    byte[] dataBytes;

    /** Future ID for synchronous event notifications. */
    @Order(4)
    IgniteUuid futId;

    /**
     * Empty constructor.
     */
    public GridContinuousMessage() {
        // No-op.
    }

    /**
     * @param type Message type.
     * @param routineId Consume ID.
     * @param futId Future ID.
     * @param data Optional message data.
     * @param msgs If {@code true} then data is collection of messages.
     */
    GridContinuousMessage(GridContinuousMessageType type,
        @Nullable UUID routineId,
        @Nullable IgniteUuid futId,
        @Nullable Object data,
        boolean msgs) {
        assert type != null;
        assert routineId != null || type == MSG_EVT_ACK;

        this.type = type;
        this.routineId = routineId;
        this.futId = futId;

        if (msgs)
            this.msgs = (Collection)data;
        else
            this.data = data;
    }

    /**
     * @return Message type.
     */
    public GridContinuousMessageType type() {
        return type;
    }

    /**
     * @return Consume ID.
     */
    public UUID routineId() {
        return routineId;
    }

    /**
     * @return {@code True} is data is collection of messages.
     */
    public boolean messages() {
        return msgs != null;
    }

    /**
     * @return Message data.
     */
    public <T> T data() {
        return msgs != null ? (T)msgs : (T)data;
    }

    /**
     * @param data Message data.
     */
    public void data(Object data) {
        this.data = data;
    }

    /**
     * @return Serialized message data.
     */
    public byte[] dataBytes() {
        return dataBytes;
    }

    /**
     * @param dataBytes Serialized message data.
     */
    public void dataBytes(byte[] dataBytes) {
        this.dataBytes = dataBytes;
    }

    /**
     * @return Future ID for synchronous event notification.
     */
    @Nullable public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 61;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridContinuousMessage.class, this);
    }
}
