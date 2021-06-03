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
import org.apache.ignite.internal.cdc.ChangeDataCapture;
import org.apache.ignite.internal.util.typedef.CI3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cdc.AbstractChangeDataCaptureTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.AbstractChangeDataCaptureTest.ChangeEventType.UPDATE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public abstract class AbstractChangeDataCaptureTest extends GridCommonAbstractTest {
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
        ChangeDataCaptureSelfTest.TestCDCConsumer cnsmr,
        ChangeDataCapture cdc,
        IgniteCache<Integer, ChangeDataCaptureSelfTest.User> cache,
        IgniteCache<Integer, ChangeDataCaptureSelfTest.User> txCache,
        CI3<IgniteCache<Integer, ChangeDataCaptureSelfTest.User>, Integer, Integer> addData,
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

        fut.cancel();

        List<Integer> keys = cnsmr.keys(UPDATE, cacheId(cache.getName()));

        assertEquals(to - from, keys.size());

        for (int i = from; i < to; i++)
            assertTrue(Integer.toString(i), keys.contains(i));

        assertTrue(cnsmr.stopped);
    }

    /** */
    public boolean waitForSize(
        int expSz,
        String cacheName,
        ChangeDataCaptureSelfTest.ChangeEventType evtType,
        long timeout,
        ChangeDataCaptureSelfTest.TestCDCConsumer... cnsmrs
    ) throws IgniteInterruptedCheckedException {
        return waitForCondition(
            () -> {
                int sum = Arrays.stream(cnsmrs).mapToInt(c -> F.size(c.keys(evtType, cacheId(cacheName)))).sum();
                return sum == expSz;
            },
            timeout);
    }

    /** */
    public ChangeDataCaptureConfiguration cdcConfig(ChangeDataCaptureConsumer cnsmr) {
        ChangeDataCaptureConfiguration cdcCfg = new ChangeDataCaptureConfiguration();

        cdcCfg.setConsumer(cnsmr);
        cdcCfg.setKeepBinary(false);

        return cdcCfg;
    }

    /** */
    public static class TestCDCConsumer implements ChangeDataCaptureConsumer {
        /** Keys */
        final ConcurrentMap<IgniteBiTuple<ChangeEventType, Integer>, List<Integer>> cacheKeys =
            new ConcurrentHashMap<>();

        /** */
        volatile boolean stopped;

        /** */
        volatile byte drId = -1;

        /** */
        volatile byte otherDrId = -1;

        /** {@inheritDoc} */
        @Override public void start() {
            stopped = false;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            stopped = true;
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<ChangeDataCaptureEvent> evts) {
            evts.forEachRemaining(evt -> {
                if (!evt.primary())
                    return;

                cacheKeys.computeIfAbsent(
                    F.t(evt.value() == null ? DELETE : UPDATE, evt.cacheId()),
                    k -> new ArrayList<>()).add((Integer)evt.key()
                );

                if (drId != -1)
                    assertEquals(drId, evt.version().dataCenterId());

                if (otherDrId != -1)
                    assertEquals(otherDrId, evt.version().otherDataCenterVersion().dataCenterId());

                if (evt.value() != null)
                    checkUser((User)evt.value());
            });

            return commit();
        }

        /** */
        protected boolean commit() {
            return true;
        }

        /** @return Read keys. */
        public List<Integer> keys(ChangeEventType op, int cacheId) {
            return cacheKeys.get(F.t(op, cacheId));
        }
    }

    /** */
    protected static User createUser(int i) {
        byte[] bytes = new byte[1024];

        ThreadLocalRandom.current().nextBytes(bytes);

        return new User(JOHN + " " + i, 42 + i, bytes);
    }

    /** */
    protected static void checkUser(User user) {
        assertTrue(user.getName().startsWith(JOHN));
        assertTrue(user.getAge() >= 42);
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
        UPDATE, DELETE
    }
}
