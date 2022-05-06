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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.AbstractCdcTest;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.UPDATE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
@RunWith(Parameterized.class)
public class SqlCdcTest extends AbstractCdcTest {
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

    /** */
    public static final String USER_KEY_TYPE = "TestUserKey";

    /** */
    public static final String USER_VAL_TYPE = "TestUser";

    /** */
    public static final String CITY_VAL_TYPE = "TestCity";

    /** */
    @Parameterized.Parameter
    public boolean persistenceEnabled;

    /** */
    @Parameterized.Parameters(name = "persistence={0}")
    public static Object[] parameters() {
        return new Object[] {false, true};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)
                .setCdcEnabled(true)));

        return cfg;
    }

    /** Simplest CDC test. */
    @Test
    public void testReadAllSQLRows() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteEx ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        BinaryCdcConsumer cnsmr = new BinaryCdcConsumer();

        CountDownLatch latch = new CountDownLatch(1);

        GridAbsPredicate userPredicate = sizePredicate(KEYS_CNT, USER, UPDATE, cnsmr);
        GridAbsPredicate cityPredicate = sizePredicate(KEYS_CNT, CITY, UPDATE, cnsmr);

        CdcMain cdc = createCdc(cnsmr, cfg, latch, userPredicate, cityPredicate);

        IgniteInternalFuture<?> fut = runAsync(cdc);

        executeSql(
            ign,
            "CREATE TABLE USER(id int, city_id int, name varchar, PRIMARY KEY (id, city_id)) " +
                "WITH \"CACHE_NAME=user,VALUE_TYPE=" + USER_VAL_TYPE + ",KEY_TYPE=" + USER_KEY_TYPE + "\""
        );

        executeSql(
            ign,
            "CREATE TABLE CITY(id int, name varchar, zip_code varchar(6), PRIMARY KEY (id)) " +
               "WITH \"CACHE_NAME=city,VALUE_TYPE=TestCity\""
        );

        for (int i = 0; i < KEYS_CNT; i++) {
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

        // Wait while both predicte will become true and state saved on the disk.
        assertTrue(latch.await(getTestTimeout(), MILLISECONDS));

        checkMetrics(cdc, KEYS_CNT * 2);

        fut.cancel();

        assertEquals(KEYS_CNT, cnsmr.data(UPDATE, cacheId(USER)).size());
        assertEquals(KEYS_CNT, cnsmr.data(UPDATE, cacheId(CITY)).size());

        assertTrue(cnsmr.stopped());

        for (int i = 0; i < KEYS_CNT; i++)
            executeSql(ign, "DELETE FROM USER WHERE id = ?", i);

        cdc = createCdc(cnsmr, cfg);

        IgniteInternalFuture<?> rmvFut = runAsync(cdc);

        waitForSize(KEYS_CNT, USER, DELETE, cnsmr);

        checkMetrics(cdc, KEYS_CNT);

        executeSql(ign, "ALTER TABLE CITY ADD COLUMN region VARCHAR");

        executeSql(
            ign,
            "INSERT INTO CITY VALUES(?, ?, ?, ?)",
            KEYS_CNT + 1,
            MSK,
            Integer.toString(127000 + KEYS_CNT + 1),
            "Moscow region");

        waitForSize(KEYS_CNT + 1, CITY, UPDATE, cnsmr);

        rmvFut.cancel();

        assertTrue(cnsmr.stopped());
    }

    /** */
    public static class BinaryCdcConsumer extends TestCdcConsumer<CdcEvent> {
        /** */
        private boolean userKeyType;

        /** */
        private boolean userValType;

        /** */
        private boolean cityValType;

        /** */
        private int mappingCnt;

        /** {@inheritDoc} */
        @Override public void checkEvent(CdcEvent evt) {
            assertTrue(userKeyType);
            assertTrue(userValType);
            assertTrue(cityValType);

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
        }

        /** {@inheritDoc} */
        @Override public CdcEvent extract(CdcEvent evt) {
            return evt;
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            assertEquals("onMappings must be executed first", 3, mappingCnt);

            while (types.hasNext()) {
                BinaryType type = types.next();

                assertNotNull(type);

                switch (type.typeName()) {
                    case USER_KEY_TYPE:
                        assertTrue(type.fieldNames().containsAll(Arrays.asList("ID", "CITY_ID")));
                        assertEquals(2, type.fieldNames().size());
                        assertEquals(int.class.getSimpleName(), type.fieldTypeName("ID"));
                        assertEquals(int.class.getSimpleName(), type.fieldTypeName("CITY_ID"));

                        userKeyType = true;

                        break;

                    case USER_VAL_TYPE:
                        assertTrue(type.fieldNames().contains("NAME"));
                        assertEquals(1, type.fieldNames().size());
                        assertEquals(String.class.getSimpleName(), type.fieldTypeName("NAME"));

                        userValType = true;

                        break;

                    case CITY_VAL_TYPE:
                        assertTrue(type.fieldNames().containsAll(Arrays.asList("NAME", "ZIP_CODE")));
                        assertEquals(cityValType ? 3 : 2, type.fieldNames().size());
                        assertEquals(String.class.getSimpleName(), type.fieldTypeName("NAME"));
                        assertEquals(String.class.getSimpleName(), type.fieldTypeName("ZIP_CODE"));

                        // Alter table happen.
                        if (cityValType) {
                            assertTrue(type.fieldNames().contains("REGION"));
                            assertEquals(String.class.getSimpleName(), type.fieldTypeName("REGION"));
                        }

                        cityValType = true;

                        break;
                    default:
                        fail("Unexpected type name " + type.typeName());
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            assertEquals(0, mappingCnt);
            assertFalse("onMappings must be executed first", cityValType || userValType || userKeyType);

            BinaryBasicIdMapper mapper = new BinaryBasicIdMapper();

            while (mappings.hasNext()) {
                mappingCnt++;

                TypeMapping m = mappings.next();

                assertNotNull(m);

                String typeName = m.typeName();

                assertFalse(typeName.isEmpty());
                assertEquals(mapper.typeId(typeName), m.typeId());
            }
        }
    }

    /** */
    private List<List<?>> executeSql(IgniteEx node, String sqlText, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sqlText).setArgs(args), true).getAll();
    }

    /** {@inheritDoc} */
    @Override protected boolean keepBinary() {
        return true;
    }
}
