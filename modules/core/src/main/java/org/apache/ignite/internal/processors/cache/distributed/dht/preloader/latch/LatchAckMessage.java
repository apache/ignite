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
package org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.codegen.LatchAckMessageSerializer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message is used to send acks for {@link Latch} instances management.
 */
public class LatchAckMessage implements Message {
    /** Latch id. */
    @Order(0)
    private String latchId;

    /** Latch topology version. */
    @Order(1)
    private AffinityTopologyVersion topVer;

    /** Flag indicates that ack is final. */
    @Order(2)
    private boolean isFinal;

    /** */
    private static final LatchAckMessageSerializer serializer = new LatchAckMessageSerializer();

    /**
     * Constructor.
     *
     * @param latchId Latch id.
     * @param topVer Latch topology version.
     * @param isFinal Final acknowledgement flag.
     */
    public LatchAckMessage(String latchId, AffinityTopologyVersion topVer, boolean isFinal) {
        this.latchId = latchId;
        this.topVer = topVer;
        this.isFinal = isFinal;
    }

    /**
     * Empty constructor for marshalling purposes.
     */
    public LatchAckMessage() {
    }

    /**
     * @return Latch id.
     */
    public String latchId() {
        return latchId;
    }

    /**
     * @param latchId New latch id.
     */
    public void latchId(String latchId) {
        this.latchId = latchId;
    }

    /**
     * @return Latch topology version.
     */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    /**
     * @param topVer New latch topology version.
     */
    public void topVer(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return {@code} if ack is final.
     */
    public boolean isFinal() {
        return isFinal;
    }

    /**
     * @param isFinal New flag indicates that ack is final.
     */
    public void isFinal(boolean isFinal) {
        this.isFinal = isFinal;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 135;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return serializer.writeTo(this, buf, writer);
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return serializer.readFrom(this, buf, reader);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
