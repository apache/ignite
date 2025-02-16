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

package org.apache.ignite.internal.dump;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.TestDumpConsumer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.cdc.SqlCdcTest.executeSql;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.DMP_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.KEYS_CNT;

/** */
public class DumpCacheConfigTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** */
    @Test
    public void testSQLTableDump() throws Exception {
        IgniteEx srv = (IgniteEx)startGridsMultiThreaded(2);

        executeSql(srv, "CREATE TABLE T1(ID INT, NAME VARCHAR, PRIMARY KEY (ID)) WITH \"CACHE_NAME=T1\"");

        for (int i = 0; i < KEYS_CNT; i++)
            executeSql(srv, "INSERT INTO T1 VALUES(?, ?)", i, "Name-" + i);

        checkDump(srv, DMP_NAME, true);

        executeSql(srv, "ALTER TABLE T1 ADD COLUMN(ADDRESS VARCHAR)");

        for (int i = KEYS_CNT; i < KEYS_CNT * 2; i++)
            executeSql(srv, "INSERT INTO T1 VALUES(?, ?, ?)", i, "Name-" + i, "Address-" + i);

        checkDump(srv, DMP_NAME + 2, false);
    }

    /** */
    private void checkDump(IgniteEx srv, String name, boolean first) throws Exception {
        srv.snapshot().createDump(name, null).get(10_000L);

        AtomicInteger cnt = new AtomicInteger();

        TestDumpConsumer cnsmr = new TestDumpConsumer() {
            @Override public void onTypes(Iterator<BinaryType> types) {
                super.onTypes(types);

                assertTrue(types.hasNext());

                BinaryType type = types.next();

                assertFalse(types.hasNext());

                assertTrue(type.typeName().startsWith("SQL_PUBLIC_T1"));
            }

            @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
                super.onCacheConfigs(caches);

                assertTrue(caches.hasNext());

                StoredCacheData data = caches.next();

                assertFalse(caches.hasNext());

                assertTrue(data.sql());

                CacheConfiguration ccfg = data.config();

                assertEquals("T1", ccfg.getName());

                Collection<QueryEntity> qes = data.queryEntities();

                assertNotNull(qes);
                assertEquals(1, qes.size());

                QueryEntity qe = qes.iterator().next();

                assertNotNull(qe);
                assertEquals("T1", qe.getTableName());
                assertEquals(first ? 2 : 3, qe.getFields().size());
                assertTrue(qe.getFields().containsKey("ID"));
                assertTrue(qe.getFields().containsKey("NAME"));
                if (!first)
                    assertTrue(qe.getFields().containsKey("ADDRESS"));
            }

            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                while (data.hasNext()) {
                    DumpEntry e = data.next();

                    assertNotNull(e);

                    BinaryObject val = (BinaryObject)e.value();

                    assertNotNull(val);
                    assertEquals("Name-" + e.key(), val.field("NAME"));

                    cnt.incrementAndGet();
                }
            }
        };

        new DumpReader(
            new DumpReaderConfiguration(new File(sharedFileTree(srv.configuration()).snapshotsRoot(), name), cnsmr),
            log
        ).run();

        assertEquals(first ? KEYS_CNT : (KEYS_CNT * 2), cnt.get());

        cnsmr.check();
    }
}
