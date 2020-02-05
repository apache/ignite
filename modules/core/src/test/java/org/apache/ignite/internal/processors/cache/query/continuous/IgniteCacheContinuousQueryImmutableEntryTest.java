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
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheContinuousQueryImmutableEntryTest extends GridCommonAbstractTest {
    /** Keys count. */
    private static final int KEYS_COUNT = 10;

    /** Grid count. */
    private static final int GRID_COUNT = 3;

    /** Events. */
    private static final ConcurrentLinkedQueue<CacheEntryEvent<?, ?>> events = new ConcurrentLinkedQueue<>();

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

        events.clear();
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

        assertTrue("There are not filtered events", !events.isEmpty());

        for (CacheEntryEvent<?, ?> event : events) {
            assertNotNull("Key is null", event.getKey());
            assertNotNull("Value is null", event.getValue());
            assertNotNull("Old value is null", event.getOldValue());
        }
    }

    /**
     *
     */
    @Test
    public void testCacheContinuousQueryEntrySerialization() {
        CacheContinuousQueryEntry e0 = new CacheContinuousQueryEntry(
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

        e0.markFiltered();

        ByteBuffer buf = ByteBuffer.allocate(4096);
        DirectMessageWriter writer = new DirectMessageWriter((byte)1);

        // Skip write class header.
        writer.onHeaderWritten();
        e0.writeTo(buf, writer);

        CacheContinuousQueryEntry e1 = new CacheContinuousQueryEntry();
        IgniteMessageFactoryImpl msgFactory =
                new IgniteMessageFactoryImpl(new MessageFactory[]{new GridIoMessageFactory()});
        e1.readFrom(ByteBuffer.wrap(buf.array()), new DirectMessageReader(msgFactory, (byte)1));

        assertEquals(e0.cacheId(), e1.cacheId());
        assertEquals(e0.eventType(), e1.eventType());
        assertEquals(e0.isFiltered(), e1.isFiltered());
        assertEquals(e0.isBackup(), e1.isBackup());
        assertEquals(e0.isKeepBinary(), e1.isKeepBinary());
        assertEquals(e0.partition(), e1.partition());
        assertEquals(e0.updateCounter(), e1.updateCounter());

        // Key and value shouldn't be serialized in case an event is filtered.
        assertNull(e1.key());
        assertNotNull(e0.key());
        assertNull(e1.oldValue());
        assertNotNull(e0.oldValue());
        assertNull(e1.value());
        assertNotNull(e0.value());
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
            events.add(evt);

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
}
