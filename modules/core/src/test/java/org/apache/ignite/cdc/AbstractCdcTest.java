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

package org.apache.ignite.cdc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import javax.management.DynamicMBean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcConsumerState;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.CI3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.UPDATE;
import static org.apache.ignite.internal.cdc.CdcMain.BINARY_META_DIR;
import static org.apache.ignite.internal.cdc.CdcMain.CDC_DIR;
import static org.apache.ignite.internal.cdc.CdcMain.COMMITTED_SEG_IDX;
import static org.apache.ignite.internal.cdc.CdcMain.COMMITTED_SEG_OFFSET;
import static org.apache.ignite.internal.cdc.CdcMain.CUR_SEG_IDX;
import static org.apache.ignite.internal.cdc.CdcMain.EVT_CAPTURE_TIME;
import static org.apache.ignite.internal.cdc.CdcMain.LAST_SEG_CONSUMPTION_TIME;
import static org.apache.ignite.internal.cdc.CdcMain.MARSHALLER_DIR;
import static org.apache.ignite.internal.cdc.CdcMain.cdcInstanceName;
import static org.apache.ignite.internal.cdc.WalRecordsConsumer.EVTS_CNT;
import static org.apache.ignite.internal.cdc.WalRecordsConsumer.LAST_EVT_TIME;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public abstract class AbstractCdcTest extends GridCommonAbstractTest {
    /** */
    public static final String JOHN = "John Connor";

    /** */
    public static final int WAL_ARCHIVE_TIMEOUT = 5_000;

    /** Keys count. */
    public static final int KEYS_CNT = 50;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    protected CdcMain createCdc(CdcConsumer cnsmr, IgniteConfiguration cfg) {
        return createCdc(cnsmr, cfg, null);
    }

    /** */
    protected CdcMain createCdc(
        CdcConsumer cnsmr,
        IgniteConfiguration cfg,
        CountDownLatch latch,
        GridAbsPredicate... conditions
    ) {
        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(cnsmr);
        cdcCfg.setKeepBinary(keepBinary());
        cdcCfg.setMetricExporterSpi(metricExporters());

        return new CdcMain(cfg, null, cdcCfg) {
            @Override protected CdcConsumerState createState(NodeFileTree ft) {
                return new CdcConsumerState(log, ft) {
                    @Override public void saveWal(T2<WALPointer, Integer> state) throws IOException {
                        super.saveWal(state);

                        if (!F.isEmpty(conditions)) {
                            for (GridAbsPredicate p : conditions) {
                                if (!p.apply())
                                    return;
                            }

                            latch.countDown();
                        }
                    }
                };
            }
        };
    }

    /** */
    protected void addAndWaitForConsumption(
        UserCdcConsumer cnsmr,
        IgniteConfiguration cfg,
        IgniteCache<Integer, CdcSelfTest.User> cache,
        IgniteCache<Integer, CdcSelfTest.User> txCache,
        CI3<IgniteCache<Integer, CdcSelfTest.User>, Integer, Integer> addData,
        int from,
        int to,
        boolean waitForCommit
    ) throws Throwable {
        GridAbsPredicate cachePredicate = sizePredicate(to - from, cache.getName(), UPDATE, cnsmr);
        GridAbsPredicate txPredicate = txCache == null
            ? null
            : sizePredicate(to - from, txCache.getName(), UPDATE, cnsmr);

        CdcMain cdc;

        CountDownLatch latch = new CountDownLatch(1);

        if (waitForCommit) {
            cdc = txCache == null
                ? createCdc(cnsmr, cfg, latch, cachePredicate)
                : createCdc(cnsmr, cfg, latch, cachePredicate, txPredicate);
        }
        else
            cdc = createCdc(cnsmr, cfg);

        IgniteInternalFuture<?> fut = runAsync(cdc);

        fut.listen(latch::countDown);

        addData.apply(cache, from, to);

        if (txCache != null)
            addData.apply(txCache, from, to);

        if (waitForCommit) {
            latch.await(getTestTimeout(), MILLISECONDS);

            if (fut.error() != null)
                throw fut.error();
        }
        else {
            assertTrue(waitForCondition(cachePredicate, getTestTimeout()));

            if (txCache != null)
                assertTrue(waitForCondition(txPredicate, getTestTimeout()));
        }

        checkMetrics(cdc, txCache == null ? to : to * 2);

        fut.cancel();

        List<Integer> keys = cnsmr.data(UPDATE, cacheId(cache.getName()));

        assertEquals(to - from, keys.size());

        for (int i = from; i < to; i++)
            assertTrue(Integer.toString(i), keys.contains(i));

        assertTrue(cnsmr.stopped());
    }

    /** */
    public void waitForSize(
        int expSz,
        String cacheName,
        CdcSelfTest.ChangeEventType evtType,
        TestCdcConsumer<?>... cnsmrs
    ) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(sizePredicate(expSz, cacheName, evtType, cnsmrs), getTestTimeout()));
    }

    /** */
    protected GridAbsPredicate sizePredicate(
        int expSz,
        String cacheName,
        ChangeEventType evtType,
        TestCdcConsumer<?>... cnsmrs
    ) {
        return () -> {
            int sum = Arrays.stream(cnsmrs).mapToInt(c -> F.size(c.data(evtType, cacheId(cacheName)))).sum();
            return sum == expSz;
        };
    }

    /** */
    protected void checkMetrics(CdcMain cdc, int expCnt) throws Exception {
        IgniteConfiguration cfg = getFieldValue(cdc, "igniteCfg");

        DynamicMBean jmxCdcReg = metricRegistry(cdcInstanceName(cfg.getIgniteInstanceName()), null, "cdc");

        Function<String, ?> jmxVal = m -> {
            try {
                return jmxCdcReg.getAttribute(m);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        };

        checkMetrics(expCnt, (Function<String, Long>)jmxVal, (Function<String, String>)jmxVal);

        MetricRegistry mreg = getFieldValue(cdc, "mreg");

        assertNotNull(mreg);

        checkMetrics(
            expCnt,
            m -> mreg.<LongMetric>findMetric(m).value(),
            m -> mreg.<ObjectMetric<String>>findMetric(m).value()
        );

        HistogramMetric evtCaptureTime = mreg.findMetric(EVT_CAPTURE_TIME);
        assertEquals(expCnt, (int)Arrays.stream(evtCaptureTime.value()).sum());
    }

    /** */
    private void checkMetrics(long expCnt, Function<String, Long> longMetric, Function<String, String> strMetric) {
        long committedSegIdx = longMetric.apply(COMMITTED_SEG_IDX);
        long curSegIdx = longMetric.apply(CUR_SEG_IDX);

        assertTrue(committedSegIdx <= curSegIdx);

        assertTrue(longMetric.apply(COMMITTED_SEG_OFFSET) >= 0);
        assertTrue(longMetric.apply(LAST_SEG_CONSUMPTION_TIME) > 0);

        assertTrue(longMetric.apply(LAST_EVT_TIME) > 0);

        for (String m : new String[] {BINARY_META_DIR, MARSHALLER_DIR, CDC_DIR})
            assertTrue(new File(strMetric.apply(m)).exists());

        assertEquals(expCnt, (long)longMetric.apply(EVTS_CNT));
    }

    /** */
    protected boolean keepBinary() {
        return false;
    }

    /** */
    protected MetricExporterSpi[] metricExporters() {
        return null;
    }

    /** */
    public abstract static class TestCdcConsumer<T> implements CdcConsumer {
        /** Keys. */
        final ConcurrentMap<IgniteBiTuple<ChangeEventType, Integer>, List<T>> data = new ConcurrentHashMap<>();

        /** Cache events. */
        protected final ConcurrentMap<Integer, CdcCacheEvent> caches = new ConcurrentHashMap<>();

        /** */
        private volatile boolean stopped;

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            stopped = false;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            stopped = true;
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<CdcEvent> evts) {
            evts.forEachRemaining(evt -> {
                if (!evt.primary())
                    return;

                data.computeIfAbsent(
                    F.t(evt.value() == null ? DELETE : UPDATE, evt.cacheId()),
                    k -> new ArrayList<>()).add(extract(evt));

                assertTrue(caches.containsKey(evt.cacheId()));

                checkEvent(evt);
            });

            return commit();
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(t -> assertNotNull(t));
        }

        /** {@inheritDoc} */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvts) {
            cacheEvts.forEachRemaining(evt -> {
                assertFalse(caches.containsKey(evt.cacheId()));

                caches.put(evt.cacheId(), evt);
            });
        }

        /** {@inheritDoc} */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            caches.forEachRemaining(cacheId -> assertNotNull(this.caches.remove(cacheId)));
        }

        /** */
        public abstract void checkEvent(CdcEvent evt);

        /** */
        public abstract T extract(CdcEvent evt);

        /** */
        protected boolean commit() {
            return true;
        }

        /** @return Read keys. */
        public List<T> data(ChangeEventType op, int cacheId) {
            return data.computeIfAbsent(F.t(op, cacheId), k -> new ArrayList<>());
        }

        /** */
        public void clear() {
            data.clear();
        }

        /** */
        public boolean stopped() {
            return stopped;
        }
    }

    /** */
    public static class UserCdcConsumer extends TestCdcConsumer<Integer> {
        /** */
        protected boolean userTypeFound;

        /** {@inheritDoc} */
        @Override public void checkEvent(CdcEvent evt) {
            assertTrue(userTypeFound);
            assertNull(evt.version().otherClusterVersion());

            if (evt.value() == null)
                return;

            User user = (User)evt.value();

            assertTrue(user.getName().startsWith(JOHN));
            assertTrue(user.getAge() >= 42);
        }

        /** {@inheritDoc} */
        @Override public Integer extract(CdcEvent evt) {
            return (Integer)evt.key();
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(t -> {
                if (t.typeName().equals(User.class.getName())) {
                    userTypeFound = true;

                    assertNotNull(t.field("name"));
                    assertEquals(String.class.getSimpleName(), t.fieldTypeName("name"));
                    assertNotNull(t.field("age"));
                    assertEquals(int.class.getName(), t.fieldTypeName("age"));
                    assertNotNull(t.field("payload"));
                    assertEquals(byte[].class.getSimpleName(), t.fieldTypeName("payload"));
                }

                assertNotNull(t);
            });
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            BinaryBasicIdMapper mapper = new BinaryBasicIdMapper();

            mappings.forEachRemaining(m -> {
                assertNotNull(m);

                String typeName = m.typeName();

                assertFalse(typeName.isEmpty());
                // Can also be registered by OptimizedMarshaller.
                assertTrue(m.typeId() == mapper.typeId(typeName) || m.typeId() == typeName.hashCode());
            });
        }
    }

    /** */
    public static class TrackCacheEventsConsumer implements CdcConsumer {
        /** Cache events. */
        public final Map<Integer, CdcCacheEvent> evts = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
            cacheEvents.forEachRemaining(e -> {
                log.info("TrackCacheEventsConsumer.add[cacheId=" + e.cacheId() + ", e=" + e + ']');
                evts.put(e.cacheId(), e);
            });
        }

        /** {@inheritDoc} */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            caches.forEachRemaining(cacheId -> {
                log.info("TrackCacheEventsConsumer.remove[cacheId=" + cacheId + ']');

                evts.remove(cacheId);
            });
        }

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<CdcEvent> evts) {
            evts.forEachRemaining(e -> { /* No-op. */ });

            return false;
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(e -> { /* No-op. */ });
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            mappings.forEachRemaining(e -> { /* No-op. */ });
        }


        /** {@inheritDoc} */
        @Override public void stop() {
            // No-op.
        }
    }

    /** */
    protected static User createUser(int i) {
        byte[] bytes = new byte[1024];

        ThreadLocalRandom.current().nextBytes(bytes);

        return new User(JOHN + " " + i, 42 + i, bytes);
    }

    /** */
    public static class User {
        /** */
        private final String name;

        /** */
        private final int age;

        /** */
        private final byte[] payload;

        /** */
        public User(String name, int age, byte[] payload) {
            this.name = name;
            this.age = age;
            this.payload = payload;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public int getAge() {
            return age;
        }

        /** */
        public byte[] getPayload() {
            return payload;
        }
    }

    /** */
    public enum ChangeEventType {
        /** */
        UPDATE,

        /** */
        DELETE
    }
}
