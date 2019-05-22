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

package org.apache.ignite.spi.communication.tcp.channel;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.GridSelectorNioSession;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.ChannelListener;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;
import org.apache.ignite.spi.communication.tcp.messages.ChannelCreateRequestMessage;

/**
 *
 */
public class IgniteSocketChannelImpl implements IgniteSocketChannel {
    /** */
    private final ConnectionKey key;

    /** */
    private final SocketChannel channel;

    /** */
    private final ChannelListener lsnr;

    /** */
    private final IgniteSocketChannelConfig config;

    /** */
    private final AtomicBoolean active = new AtomicBoolean();

    /** */
    private byte plc;

    /** */
    private Object topic;

    /**
     * @param key Connection key.
     * @param channel The {@link SocketChannel} which will be used.
     */
    public IgniteSocketChannelImpl(ConnectionKey key, SocketChannel channel, ChannelListener lsnr) {
        this.key = key;
        this.channel = channel;
        this.config = new IgniteSocketChannelConfig(channel);
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return key.nodeId();
    }

    /** {@inheritDoc} */
    @Override public int id() {
        return key.connectionIndex();
    }

    /** {@inheritDoc} */
    @Override public SocketChannel channel() {
        return channel;
    }

    /**
     * @param ses The nio session to send configure request over it.
     * @param msg The configuration channel message.
     * @throws IgniteCheckedException If fails.
     */
    public void configure(GridSelectorNioSession ses, Message msg) throws IgniteCheckedException {
        assert ses.key().channel() == channel;

        ses.send(new ChannelCreateRequestMessage(msg)).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteSocketChannelConfig config() {
        return config;
    }

    /** {@inheritDoc} */
    @Override public boolean active() {
        return active.get();
    }

    /** {@inheritDoc} */
    @Override public void activate() {
        boolean res = active.compareAndSet(false, true);

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
    @Override public void close() throws IOException {
        lsnr.onChannelClose(this);

        U.closeQuiet(channel);
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
        return S.toString(IgniteSocketChannelImpl.class, this);
    }
}
