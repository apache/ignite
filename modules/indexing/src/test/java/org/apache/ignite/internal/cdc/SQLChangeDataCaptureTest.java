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

package org.apache.ignite.internal.cdc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.ChangeDataCaptureConfiguration;
import org.apache.ignite.cdc.ChangeDataCaptureConsumer;
import org.apache.ignite.cdc.ChangeDataCaptureEvent;
import org.apache.ignite.cdc.ChangeDataCaptureSelfTest.ChangeEventType;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cdc.ChangeDataCaptureSelfTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.ChangeDataCaptureSelfTest.ChangeEventType.UPDATE;
import static org.apache.ignite.cdc.ChangeDataCaptureSelfTest.WAL_ARCHIVE_TIMEOUT;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAbstractTransformWriteThroughSelfTest.KEYS_CNT;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class SQLChangeDataCaptureTest extends GridCommonAbstractTest {
    /** */
    private static final String JOHN = "John Connor";

    /** */
    private static final String SARAH = "Sarah Connor";

    /** */
    public static final String USER = "user";

    /** */
    public static final String CITY = "city";

    /** */
    public static final String SPB = "Saint-Petersburg";

    /** */
    public static final String MSK = "Moscow";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int segmentSz = 10 * 1024 * 1024;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setChangeDataCaptureEnabled(true)
            .setWalMode(WALMode.FSYNC)
            .setMaxWalArchiveSize(10 * segmentSz)
            .setWalSegmentSize(segmentSz)
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** Simplest CDC test. */
    @Test
    public void testReadAllSQLRows() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteEx ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        BinaryCDCConsumer cnsmr = new BinaryCDCConsumer();

        ChangeDataCaptureConfiguration cdcCfg = new ChangeDataCaptureConfiguration();

        cdcCfg.setConsumer(cnsmr);
        cdcCfg.setKeepBinary(true);

        ChangeDataCapture cdc = new ChangeDataCapture(cfg, null, cdcCfg);

        IgniteInternalFuture<?> fut = runAsync(cdc);

        executeSql(
            ign,
            "CREATE TABLE USER(id int, city_id int, name varchar, PRIMARY KEY (id, city_id)) WITH \"CACHE_NAME=user\""
        );

        executeSql(
            ign,
            "CREATE TABLE CITY(id int, name varchar, zip_code varchar(6), PRIMARY KEY (id)) WITH \"CACHE_NAME=city\""
        );

        for (int i = 0; i < KEYS_CNT * 2; i++) {
            executeSql(
                ign,
                "INSERT INTO USER VALUES(?, ?, ?)",
                i,
                42 * i,
                (i % 2 == 0 ? JOHN : SARAH) + i);

            executeSql(
                ign,
                "INSERT INTO CITY VALUES(?, ?, ?)",
                i,
                (i % 2 == 0 ? MSK : SPB) + i,
                Integer.toString(127000 + i));
        }

        assertTrue(waitForSize(KEYS_CNT * 2, USER, UPDATE, cnsmr));
        assertTrue(waitForSize(KEYS_CNT * 2, CITY, UPDATE, cnsmr));

        fut.cancel();

        assertEquals(KEYS_CNT * 2, cnsmr.data(UPDATE, cacheId(USER)).size());
        assertEquals(KEYS_CNT * 2, cnsmr.data(UPDATE, cacheId(CITY)).size());

        assertTrue(cnsmr.stopped);

        for (int i = 0; i < KEYS_CNT; i++)
            executeSql(ign, "DELETE FROM USER WHERE id = ?", i);

        IgniteInternalFuture<?> rmvFut = runAsync(cdc);

        assertTrue(waitForSize(KEYS_CNT, USER, DELETE, cnsmr));

        rmvFut.cancel();

        assertTrue(cnsmr.stopped);
    }

    /** */
    private boolean waitForSize(
        int expSz,
        String cacheName,
        ChangeEventType evtType,
        BinaryCDCConsumer cnsmr
    ) throws IgniteInterruptedCheckedException {
        return waitForCondition(
            () -> F.size(cnsmr.data(evtType, cacheId(cacheName))) == expSz,
            getTestTimeout());
    }

    /** */
    private static class BinaryCDCConsumer implements ChangeDataCaptureConsumer {
        /** Keys */
        private final ConcurrentMap<IgniteBiTuple<ChangeEventType, Integer>, List<ChangeDataCaptureEvent>> cacheKeys =
            new ConcurrentHashMap<>();

        /** */
        public volatile boolean stopped;

        /** {@inheritDoc} */
        @Override public void start() {
            stopped = false;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            stopped = true;
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<ChangeDataCaptureEvent> events) {
            events.forEachRemaining(evt -> {
                if (!evt.primary())
                    return;

                cacheKeys.computeIfAbsent(
                    F.t(evt.value() == null ? DELETE : UPDATE, evt.cacheId()),
                    k -> new ArrayList<>()
                ).add(evt);

                if (evt.value() == null)
                    return;

                if (evt.cacheId() == cacheId(USER)) {
                    int id = ((BinaryObject)evt.key()).field("ID");
                    int cityId = ((BinaryObject)evt.key()).field("CITY_ID");

                    assertEquals(42 * id, cityId);

                    String name = ((BinaryObject)evt.value()).field("NAME");

                    if (id % 2 == 0)
                        assertTrue(name.startsWith(JOHN));
                    else
                        assertTrue(name.startsWith(SARAH));
                }
                else {
                    int id = (Integer)evt.key();
                    String name = ((BinaryObject)evt.value()).field("NAME");
                    String zipCode = ((BinaryObject)evt.value()).field("ZIP_CODE");

                    assertEquals(Integer.toString(127000 + id), zipCode);

                    if (id % 2 == 0)
                        assertTrue(name.startsWith(MSK));
                    else
                        assertTrue(name.startsWith(SPB));
                }
            });

            return true;
        }

        /** */
        public List<ChangeDataCaptureEvent> data(ChangeEventType op, int cacheId) {
            return cacheKeys.get(F.t(op, cacheId));
        }
    }

    /** */
    private List<List<?>> executeSql(IgniteEx node, String sqlText, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sqlText).setArgs(args), true).getAll();
    }
}
