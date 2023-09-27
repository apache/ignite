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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import org.apache.ignite.cache.query.CacheEntryEventAdapter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.client.ClientDisconnectListener;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryByteBufferInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.client.thin.ClientNotificationType.CONTINUOUS_QUERY_EVENT;
import static org.apache.ignite.internal.client.thin.TcpClientCache.JAVA_PLATFORM;

/**
 * Handler for {@link ContinuousQuery} listeners and JCache cache entry listeners.
 */
public class ClientCacheEntryListenerHandler<K, V> implements NotificationListener, AutoCloseable {
    /** "Keep binary" flag mask. */
    private static final byte KEEP_BINARY_FLAG_MASK = 0x01;

    /** */
    private final Cache<K, V> jCacheAdapter;

    /** */
    private final ReliableChannel ch;

    /** */
    private final boolean keepBinary;

    /** */
    private final ClientUtils utils;

    /** */
    private volatile CacheEntryUpdatedListener<K, V> locLsnr;

    /** */
    private volatile ClientDisconnectListener disconnectLsnr;

    /** */
    private volatile ClientChannel clientCh;

    /** */
    private volatile Long rsrcId;

    /** */
    ClientCacheEntryListenerHandler(
        Cache<K, V> jCacheAdapter,
        ReliableChannel ch,
        ClientBinaryMarshaller marsh,
        boolean keepBinary
    ) {
        this.jCacheAdapter = jCacheAdapter;
        this.ch = ch;
        this.keepBinary = keepBinary;
        utils = new ClientUtils(marsh);
    }

    /**
     * Send request to the server and start
     */
    public synchronized void startListen(
        CacheEntryUpdatedListener<K, V> locLsnr,
        ClientDisconnectListener disconnectLsnr,
        Factory<? extends CacheEntryEventFilter<? super K, ? super V>> rmtFilterFactory,
        int pageSize,
        long timeInterval,
        boolean includeExpired
    ) {
        assert locLsnr != null;

        if (clientCh != null)
            throw new IllegalStateException("Listener was already started");

        this.locLsnr = locLsnr;
        this.disconnectLsnr = disconnectLsnr;

        Consumer<PayloadOutputChannel> qryWriter = payloadCh -> {
            BinaryOutputStream out = payloadCh.out();

            out.writeInt(ClientUtils.cacheId(jCacheAdapter.getName()));
            out.writeByte(keepBinary ? KEEP_BINARY_FLAG_MASK : 0);
            out.writeInt(pageSize);
            out.writeLong(timeInterval);
            out.writeBoolean(includeExpired);

            if (rmtFilterFactory == null)
                out.writeByte(GridBinaryMarshaller.NULL);
            else {
                utils.writeObject(out, rmtFilterFactory);
                out.writeByte(JAVA_PLATFORM);
            }
        };

        Function<PayloadInputChannel, T2<ClientChannel, Long>> qryReader = payloadCh -> {
            ClientChannel ch = payloadCh.clientChannel();
            Long rsrcId = payloadCh.in().readLong();

            ch.addNotificationListener(CONTINUOUS_QUERY_EVENT, rsrcId, this);

            return new T2<>(ch, rsrcId);
        };

        try {
            T2<ClientChannel, Long> params = ch.service(ClientOperation.QUERY_CONTINUOUS, qryWriter, qryReader);

            clientCh = params.get1();
            rsrcId = params.get2();
        }
        catch (ClientError e) {
            throw new ClientException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptNotification(ByteBuffer payload, Exception err) {
        if (err == null && payload != null) {
            BinaryInputStream in = BinaryByteBufferInputStream.create(payload);

            int cnt = in.readInt();

            List<CacheEntryEvent<? extends K, ? extends V>> evts = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++) {
                K key = utils.readObject(in, keepBinary);
                V oldVal = utils.readObject(in, keepBinary);
                V val = utils.readObject(in, keepBinary);
                byte evtTypeByte = in.readByte();

                EventType evtType = eventType(evtTypeByte);

                if (evtType == null)
                    onChannelClosed(new ClientException("Unknown event type: " + evtTypeByte));

                evts.add(new CacheEntryEventImpl<>(jCacheAdapter, evtType, key, oldVal, val));
            }

            locLsnr.onUpdated(evts);
        }
    }

    /** {@inheritDoc} */
    @Override public void onChannelClosed(Exception reason) {
        ClientDisconnectListener lsnr = disconnectLsnr;

        if (lsnr != null)
            lsnr.onDisconnected(reason);

        U.closeQuiet(this);
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        ClientChannel clientCh = this.clientCh;

        if (clientCh != null && !clientCh.closed()) {
            clientCh.removeNotificationListener(CONTINUOUS_QUERY_EVENT, rsrcId);

            clientCh.service(ClientOperation.RESOURCE_CLOSE, ch -> ch.out().writeLong(rsrcId), null);
        }
    }

    /**
     * Client channel.
     */
    public ClientChannel clientChannel() {
        return clientCh;
    }

    /** */
    private EventType eventType(byte evtTypeByte) {
        switch (evtTypeByte) {
            case 0: return EventType.CREATED;
            case 1: return EventType.UPDATED;
            case 2: return EventType.REMOVED;
            case 3: return EventType.EXPIRED;
            default: return null;
        }
    }

    /**
     *
     */
    private static class CacheEntryEventImpl<K, V> extends CacheEntryEventAdapter<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key. */
        private final K key;

        /** Old value. */
        private final V oldVal;

        /** Value. */
        private final V newVal;

        /**
         *
         */
        private CacheEntryEventImpl(Cache<K, V> src, EventType evtType, K key, V oldVal, V newVal) {
            super(src, evtType);

            this.key = key;
            this.oldVal = oldVal;
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override protected V getNewValue() {
            return newVal;
        }

        /** {@inheritDoc} */
        @Override public V getOldValue() {
            return oldVal;
        }

        /** {@inheritDoc} */
        @Override public boolean isOldValueAvailable() {
            return oldVal != null;
        }
    }
}
