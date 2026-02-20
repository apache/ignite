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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;

/**
 *
 */
public class IgniteCacheContinuousQueryImmutableEntryTest extends GridCommonAbstractTest {
    /** Message factory. */
    protected final MessageFactory msgFactory =
        new IgniteMessageFactoryImpl(new MessageFactoryProvider[] {new GridIoMessageFactory()});

    /** Keys count. */
    private static final int KEYS_COUNT = 10;

    /** Grid count. */
    private static final int GRID_COUNT = 3;

    /** Events. */
    private static final ConcurrentLinkedQueue<CacheEntryEvent<?, ?>> evts = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        evts.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEventAvailabilityScope() throws Exception {
        startGrids(GRID_COUNT);

        final CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();
        qry.setLocalListener(lsnr);
        qry.setRemoteFilterFactory(new FilterFactory());

        Object keys[] = new Object[GRID_COUNT];

        // Add initial values.
        for (int i = 0; i < GRID_COUNT; ++i) {
            keys[i] = primaryKey(grid(i).cache(DEFAULT_CACHE_NAME));

            grid(0).cache(DEFAULT_CACHE_NAME).put(keys[i], -1);
        }

        try (QueryCursor<?> cur = grid(0).cache(DEFAULT_CACHE_NAME).query(qry)) {
            // Replace values on the keys.
            for (int i = 0; i < KEYS_COUNT; i++) {
                log.info("Put key: " + i);

                grid(i % GRID_COUNT).cache(DEFAULT_CACHE_NAME).put(keys[i % GRID_COUNT], i);
            }
        }

        assertTrue("There are not filtered events", !evts.isEmpty());

        for (CacheEntryEvent<?, ?> evt : evts) {
            assertNotNull("Key is null", evt.getKey());
            assertNotNull("Value is null", evt.getValue());
            assertNotNull("Old value is null", evt.getOldValue());
        }
    }

    /**
     *
     */
    @Test
    public void testCacheContinuousQueryEntrySerialization() {
        var srcMsg = new CacheContinuousQueryEntry(
            1,
            EventType.UPDATED,
            new KeyCacheObjectImpl(1, new byte[] {0, 0, 0, 1}, 1),
            new CacheObjectImpl(2, new byte[] {0, 0, 0, 2}),
            new CacheObjectImpl(2, new byte[] {0, 0, 0, 3}),
            true,
            1,
            1L,
            new AffinityTopologyVersion(1L),
            (byte)0);

        srcMsg.markFiltered();

        srcMsg.prepare(new GridDeploymentInfoBean(
            IgniteUuid.randomUuid(), "", CONTINUOUS, Map.of()
        ));

        var resMsg = doMarshalUnmarshal(srcMsg);

        assertEquals(srcMsg.cacheId(), resMsg.cacheId());
        assertEquals(srcMsg.eventType(), resMsg.eventType());
        assertEquals(srcMsg.isFiltered(), resMsg.isFiltered());
        assertEquals(srcMsg.isBackup(), resMsg.isBackup());
        assertEquals(srcMsg.isKeepBinary(), resMsg.isKeepBinary());
        assertEquals(srcMsg.partition(), resMsg.partition());
        assertEquals(srcMsg.updateCounter(), resMsg.updateCounter());

        // Key and value shouldn't be serialized in case an event is filtered.
        assertNull(resMsg.key());
        assertNotNull(srcMsg.key());
        assertNull(resMsg.oldValue());
        assertNotNull(srcMsg.oldValue());
        assertNull(resMsg.newValue());
        assertNotNull(srcMsg.newValue());
        assertNull(resMsg.deployInfo()); // Transient
    }

    /**
     *
     */
    private static class FilterFactory implements Factory<CacheEntryEventFilter<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryEventFilter<Object, Object> create() {
            return new CacheEventFilter();
        }
    }

    /**
     *
     */
    private static class CacheEventFilter implements CacheEntryEventFilter<Object, Object>, Serializable {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) {
            evts.add(evt);

            return false;
        }
    }

    /**
     *
     */
    private static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            // No-op
        }
    }

    /**
     * Performs a full marshal-unmarshal round trip on a message using {@link MessageSerializer}.
     * This method simulates incremental writes/reads that can occur in network communication
     * by gradually increasing buffer size until the operation succeeds.
     *
     * @param srcMsg Source message to serialize.
     * @param <T> Message type.
     * @return Deserialized message.
     */
    protected <T extends Message> T doMarshalUnmarshal(T srcMsg) {
        var buf = ByteBuffer.allocate(8 * 1024);

        MessageSerializer serializer = msgFactory.serializer(srcMsg.directType());
        assertNotNull("Serializer not found for message type " + srcMsg.directType(), serializer);

        // Write phase
        var fullyWritten = loopBuffer(buf, 0, wBuf -> {
            var writer = new DirectMessageWriter(msgFactory);
            writer.setBuffer(wBuf);
            return serializer.writeTo(srcMsg, writer);
        });
        assertTrue("The message was not written completely.", fullyWritten);

        // Read type code (little-endian) and create message
        buf.flip();

        byte b0 = buf.get();
        byte b1 = buf.get();

        var type = (short)((b1 & 0xFF) << 8 | b0 & 0xFF);
        assertEquals("Message type mismatch", srcMsg.directType(), type);

        var resMsg = (T)msgFactory.create(type);
        assertNotNull("Failed to create message for type " + type, resMsg);

        // Read phase
        var fullyRead = loopBuffer(buf, buf.position(), rBuf -> {
            var reader = new DirectMessageReader(msgFactory, null);
            reader.setBuffer(rBuf);
            return serializer.readFrom(resMsg, reader);
        });
        assertTrue("The message was not read completely.", fullyRead);

        return resMsg;
    }

    /**
     * Loops over incrementally larger buffer sizes until the function succeeds.
     * This simulates partial writes/reads that can occur in network communication.
     *
     * @param buf Byte buffer.
     * @param start Start position.
     * @param func Function that is sequentially executed on a different-sized part of the buffer.
     * @return {@code True} if the function returns {@code True} at least once, {@code False} otherwise.
     */
    private boolean loopBuffer(ByteBuffer buf, int start, Function<ByteBuffer, Boolean> func) {
        int pos = start;

        do {
            buf.position(start);
            buf.limit(++pos);

            if (func.apply(buf))
                return true;
        }
        while (pos < buf.capacity());

        return false;
    }

}
