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

package org.apache.ignite.internal.client.thin;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Channels holder.
 */
class ClientChannelHolder {
    /** Channel configuration. */
    final ClientChannelConfiguration chCfg;

    /** Channel. */
    private volatile ClientChannel ch;

    /** Address that holder is bind to (chCfg.addr) is not in use now. So close the holder */
    private volatile boolean close;

    /** Timestamps of reconnect retries. */
    private final long[] reconnectRetries;

    /** Channel factory. */
    private final Function<ClientChannelConfiguration, ClientChannel> chFactory;

    /** Callback invokes when new channel create */
    private final BiConsumer<ClientChannelHolder, ClientChannel> onChannelCreate;

    /** Callback invokes when channel close */
    private final Consumer<ClientChannel> onChannelClose;

    /**
     * @param chCfg Channel config.
     */
    ClientChannelHolder(ClientChannelConfiguration chCfg,
                        Function<ClientChannelConfiguration, ClientChannel> chFactory,
                        BiConsumer<ClientChannelHolder, ClientChannel> onChannelCreate,
                        Consumer<ClientChannel> onChannelClose) {
        this.chCfg = chCfg;
        this.chFactory = chFactory;
        this.onChannelCreate = onChannelCreate;
        this.onChannelClose = onChannelClose;

        reconnectRetries = chCfg.getReconnectThrottlingRetries() > 0 && chCfg.getReconnectThrottlingPeriod() > 0L ?
            new long[chCfg.getReconnectThrottlingRetries()] : null;
    }

    /**
     * @return Whether reconnect throttling should be applied.
     */
    boolean applyReconnectionThrottling() {
        if (reconnectRetries == null)
            return false;

        long ts = System.currentTimeMillis();

        for (int i = 0; i < reconnectRetries.length; i++) {
            if (ts - reconnectRetries[i] >= chCfg.getReconnectThrottlingPeriod()) {
                reconnectRetries[i] = ts;

                return false;
            }
        }

        return true;
    }

    /**
     * Get or create channel.
     */
    ClientChannel getOrCreateChannel()
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        return getOrCreateChannel(false);
    }

    /**
     * Get or create channel.
     */
    ClientChannel getOrCreateChannel(boolean ignoreThrottling)
        throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
        if (ch == null && !close) {
            synchronized (this) {
                if (ch != null)
                    return ch;

                if (close)
                    return null;

                if (!ignoreThrottling && applyReconnectionThrottling())
                    throw new ClientConnectionException("Reconnect is not allowed due to applied throttling");

                ClientChannel channel = chFactory.apply(chCfg);

                if (channel.serverNodeId() != null)
                    onChannelCreate.accept(this, channel);

                ch = channel;
            }
        }

        return ch;
    }

    /** Get channel */
    ClientChannel getChannel() {
        return ch;
    }

    /** Close holder. */
    void close() {
        close = true;
        closeChannel();
    }

    /**
     * Close channel.
     */
    void closeChannel() {
        if (ch != null) {
            synchronized (this) {
                if (ch != null) {
                    U.closeQuiet(ch);
                    onChannelClose.accept(ch);
                    ch = null;
                }
            }
        }
    }
}
