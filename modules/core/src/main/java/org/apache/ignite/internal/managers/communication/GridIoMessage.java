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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.ExecutorAwareMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.processors.tracing.messages.SpanTransport;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for all grid messages.
 */
public class GridIoMessage implements Message, SpanTransport {
    /** */
    public static final Integer STRIPE_DISABLED_PART = Integer.MIN_VALUE;

    /** Policy. */
    @Order(0)
    byte plc;

    /** Message topic. */
    @GridToStringInclude
    private Object topic;

    /** Topic bytes. */
    @Order(1)
    byte[] topicBytes;

    /** Topic ordinal. */
    @Order(2)
    int topicOrd = -1;

    /** Message ordered flag. */
    @Order(3)
    boolean ordered;

    /** Message timeout. */
    @Order(4)
    long timeout;

    /** Whether message can be skipped on timeout. */
    @Order(5)
    boolean skipOnTimeout;

    /** Message. */
    @Order(6)
    Message msg;

    /** Serialized span */
    @Order(7)
    byte[] span;

    /**
     * Default constructor.
     */
    public GridIoMessage() {
        // No-op.
    }

    /**
     * @param plc Policy.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Message.
     * @param ordered Message ordered flag.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     */
    public GridIoMessage(
        byte plc,
        Object topic,
        int topicOrd,
        Message msg,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout
    ) {
        assert topic != null;
        assert topicOrd <= Byte.MAX_VALUE;
        assert msg != null;

        this.plc = plc;
        this.msg = msg;
        this.topic = topic;
        this.topicOrd = topicOrd;
        this.ordered = ordered;
        this.timeout = timeout;
        this.skipOnTimeout = skipOnTimeout;
    }

    /**
     * @return Policy.
     */
    public byte policy() {
        return plc;
    }

    /**
     * @param plc Policy.
     */
    public void policy(byte plc) {
        this.plc = plc;
    }

    /**
     * @return Topic.
     */
    Object topic() {
        return topic;
    }

    /**
     * @param topic Topic.
     */
    void topic(Object topic) {
        this.topic = topic;
    }

    /**
     * @return Topic bytes.
     */
    public byte[] topicBytes() {
        return topicBytes;
    }

    /**
     * @param topicBytes Topic bytes.
     */
    public void topicBytes(byte[] topicBytes) {
        this.topicBytes = topicBytes;
    }

    /**
     * @return Topic ordinal.
     */
    public int topicOrdinal() {
        return topicOrd;
    }

    /**
     * @param topicOrd Topic ordinal.
     */
    public void topicOrdinal(int topicOrd) {
        this.topicOrd = topicOrd;
    }

    /**
     * @return Message.
     */
    public Message message() {
        return msg;
    }

    /**
     * @param msg Message.
     */
    public void message(Message msg) {
        this.msg = msg;
    }

    /**
     * @return Message timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout Message timeout.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Whether message can be skipped on timeout.
     */
    public boolean skipOnTimeout() {
        return skipOnTimeout;
    }

    /**
     * @param skipOnTimeout Whether message can be skipped on timeout.
     */
    public void skipOnTimeout(boolean skipOnTimeout) {
        this.skipOnTimeout = skipOnTimeout;
    }

    /**
     * @return {@code True} if message is ordered, {@code false} otherwise.
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * @param ordered {@code True} if message is ordered, {@code false} otherwise.
     */
    public void isOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public void span(byte[] span) {
        this.span = span;
    }

    /** {@inheritDoc} */
    @Override public byte[] span() {
        return span;
    }

    /**
     * Get single partition for this message (if applicable).
     *
     * @return Partition ID.
     */
    public int partition() {
        if (msg instanceof GridCacheMessage)
            return ((GridCacheMessage)msg).partition();
        if (msg instanceof DataStreamerRequest)
            return ((DataStreamerRequest)msg).partition();
        else
            return STRIPE_DISABLED_PART;
    }

    /**
     * @return Executor name (if available).
     */
    @Nullable public String executorName() {
        if (msg instanceof ExecutorAwareMessage)
            return ((ExecutorAwareMessage)msg).executorName();

        return null;
    }

    /**
     * @param marsh Marshaller.
     */
    public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (topic != null && topicBytes == null)
            topicBytes = U.marshal(marsh, topic);
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (topicBytes != null && topic == null) {
            topic = U.unmarshal(marsh, topicBytes, ldr);

            topicBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIoMessage.class, this);
    }
}
