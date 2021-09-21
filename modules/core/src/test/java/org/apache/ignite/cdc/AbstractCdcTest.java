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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.CI3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.UPDATE;
import static org.apache.ignite.internal.cdc.CdcMain.COMMITTED_SEG_IDX;
import static org.apache.ignite.internal.cdc.CdcMain.COMMITTED_SEG_OFF;
import static org.apache.ignite.internal.cdc.CdcMain.CUR_SEG_IDX;
import static org.apache.ignite.internal.cdc.CdcMain.LAST_SEG_CONSUMPTION_TIME;
import static org.apache.ignite.internal.cdc.WalRecordsConsumer.EVTS_CNT;
import static org.apache.ignite.internal.cdc.WalRecordsConsumer.LAST_EVT_TIME;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
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

    /** */
    public void addAndWaitForConsumption(
        UserCdcConsumer cnsmr,
        CdcMain cdc,
        IgniteCache<Integer, CdcSelfTest.User> cache,
        IgniteCache<Integer, CdcSelfTest.User> txCache,
        CI3<IgniteCache<Integer, CdcSelfTest.User>, Integer, Integer> addData,
        int from,
        int to,
        long timeout
    ) throws IgniteCheckedException {
        IgniteInternalFuture<?> fut = runAsync(cdc);

        addData.apply(cache, from, to);

        if (txCache != null)
            addData.apply(txCache, from, to);

        assertTrue(waitForSize(to - from, cache.getName(), UPDATE, timeout, cnsmr));

        if (txCache != null)
            assertTrue(waitForSize(to - from, txCache.getName(), UPDATE, timeout, cnsmr));

        checkMetrics(cdc, txCache == null ? to : to * 2);

        fut.cancel();

        List<Integer> keys = cnsmr.data(UPDATE, cacheId(cache.getName()));

        assertEquals(to - from, keys.size());

        for (int i = from; i < to; i++)
            assertTrue(Integer.toString(i), keys.contains(i));

        assertTrue(cnsmr.stopped());
    }

    /** */
    public boolean waitForSize(
        int expSz,
        String cacheName,
        CdcSelfTest.ChangeEventType evtType,
        long timeout,
        TestCdcConsumer<?>... cnsmrs
    ) throws IgniteInterruptedCheckedException {
        return waitForCondition(
            () -> {
                int sum = Arrays.stream(cnsmrs).mapToInt(c -> F.size(c.data(evtType, cacheId(cacheName)))).sum();
                return sum == expSz;
            },
            timeout);
    }

    /** */
    public long checkMetrics(CdcMain cdc, int expCnt) {
        MetricRegistry mreg = GridTestUtils.getFieldValue(cdc, "mreg");

        assertNotNull(mreg);

        long committedSegIdx = mreg.<LongMetric>findMetric(COMMITTED_SEG_IDX).value();
        long curSegIdx = mreg.<LongMetric>findMetric(CUR_SEG_IDX).value();

        assertTrue(committedSegIdx <= curSegIdx);

        assertTrue(mreg.<LongMetric>findMetric(COMMITTED_SEG_OFF).value() >= 0);
        assertTrue(mreg.<LongMetric>findMetric(LAST_SEG_CONSUMPTION_TIME).value() > 0);

        assertTrue(mreg.<LongMetric>findMetric(LAST_EVT_TIME).value() > 0);

        if (expCnt != -1)
            assertTrue(mreg.<LongMetric>findMetric(EVTS_CNT).value() >= expCnt);

        return mreg.<LongMetric>findMetric(EVTS_CNT).value();
    }

    /** */
    public CdcConfiguration cdcConfig(CdcConsumer cnsmr) {
        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(cnsmr);
        cdcCfg.setKeepBinary(false);
        cdcCfg.setMetricExporterSpi(metricExporters());

        return cdcCfg;
    }

    /** */
    public MetricExporterSpi[] metricExporters() {
        return null;
    }

    /** */
    public abstract static class TestCdcConsumer<T> implements CdcConsumer {
        /** Keys */
        final ConcurrentMap<IgniteBiTuple<ChangeEventType, Integer>, List<T>> data = new ConcurrentHashMap<>();

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

                checkEvent(evt);
            });

            return commit();
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
            return data.get(F.t(op, cacheId));
        }

        /** */
        public boolean stopped() {
            return stopped;
        }
    }

    /** */
    public static class UserCdcConsumer extends TestCdcConsumer<Integer> {
        /** {@inheritDoc} */
        @Override public void checkEvent(CdcEvent evt) {
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
