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

package org.apache.ignite.internal.util.nio.channel;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.GridNioFilterChain;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

/**
 *
 */
public class IgniteSocketChannelImpl implements IgniteSocketChannel {
    /** */
    private final ConnectionKey key;

    /** */
    private final SocketChannel channel;

    /** */
    private final GridNioFilterChain filterChain;

    /** */
    private final IgniteSocketChannelConfig config;

    /** */
    private final AtomicBoolean readyStatus = new AtomicBoolean();

    /** */
    private byte plc;

    /** */
    private Object topic;

    /** */
    private int grpId;

    /**
     * Create a new NIO socket channel.
     *
     * @param key Connection key.
     * @param channel The {@link SocketChannel} which will be used.
     */
    public IgniteSocketChannelImpl(ConnectionKey key, SocketChannel channel, GridNioFilterChain filterChain) {
        this.key = key;
        this.channel = channel;
        this.config = new IgniteSocketChannelConfig(channel);
        this.filterChain = filterChain;
    }

    /** {@inheritDoc} */
    @Override public ConnectionKey id() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public SocketChannel channel() {
        return channel;
    }

    /** {@inheritDoc} */
    @Override public IgniteSocketChannelConfig config() {
        return config;
    }

    /** {@inheritDoc} */
    @Override public boolean ready() {
        return readyStatus.get();
    }

    /** {@inheritDoc} */
    @Override public void setReady() {
        boolean res = readyStatus.compareAndSet(false, true);

        assert res;
    }

    /** {@inheritDoc} */
    @Override public byte policy() {
        return plc;
    }

    /** {@inheritDoc} */
    @Override public void policy(byte plc) {
        this.plc = plc;
    }

    /** {@inheritDoc} */
    @Override public Object topic() {
        return topic;
    }

    /** {@inheritDoc} */
    @Override public void topic(Object topic) {
        this.topic = topic;
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return grpId;
    }

    /** {@inheritDoc} */
    @Override public void groupId(int grpId) {
        this.grpId = grpId;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        try {
            filterChain.onChannelClose(this);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IgniteSocketChannelImpl channel1 = (IgniteSocketChannelImpl)o;

        if (!key.equals(channel1.key))
            return false;
        return channel.equals(channel1.channel);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + channel.hashCode();
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteSocketChannelImpl{" +
            "key=" + key +
            ", channel=" + channel +
            ", config=" + config +
            ", policy=" + plc +
            ", topic=" + topic +
            ", grpId=" + grpId +
            '}';
    }
}
