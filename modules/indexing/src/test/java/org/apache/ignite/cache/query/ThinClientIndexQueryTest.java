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

package org.apache.ignite.cache.query;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
@RunWith(Parameterized.class)
public class ThinClientIndexQueryTest extends GridCommonAbstractTest {
    /** */
    private static final int CNT = 10_000;

    /** */
    private static final int NODES = 2;

    /** */
    private static final String IDX_FLD1 = "IDX_FLD1";

    /** */
    private static final String IDX_FLD1_FLD2 = "IDX_FLD1_FLD2";

    /** */
    @Parameterized.Parameter
    public boolean keepBinary;

    /** */
    @Parameterized.Parameters(name = "keepBinary={0}")
    public static Object[] params() {
        return new Object[] { false, true };
    }

    /** @inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration ccfg = super.getConfiguration(instanceName);

        ccfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite crd = startGrids(NODES);

        crd.getOrCreateCache(new CacheConfiguration<Integer, Person>()
            .setName("CACHE")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, Person.class));

        try (IgniteDataStreamer<Integer, Person> stream = grid(0).dataStreamer("CACHE")) {
            for (int i = 0; i < CNT; i++)
                stream.addData(i, new Person(i, i));
        }
    }

    /** */
    @Test
    public void testValidRanges() {
        Random rnd = new Random();

        int left = rnd.nextInt(CNT / 2);
        int right = CNT / 2 + rnd.nextInt(CNT / 4) + 1;

        for (String idxName: F.asList(IDX_FLD1, IDX_FLD1_FLD2, null)) {
            withClientCache((cache) -> {
                // No criteria.
                assertClientQuery(cache, 0, CNT, idxName);

                // Single field, single criterion.
                assertClientQuery(cache, left + 1, CNT, idxName, gt("fld1", left));
                assertClientQuery(cache, left, CNT, idxName, gte("fld1", left));
                assertClientQuery(cache, 0, left, idxName, lt("fld1", left));
                assertClientQuery(cache, 0, left + 1, idxName, lte("fld1", left));
                assertClientQuery(cache, left, left + 1, idxName, eq("fld1", left));
                assertClientQuery(cache, left, right, idxName, between("fld1", left, right));

                // Single field, multiple criteria.
                assertClientQuery(cache, left, right + 1, idxName, gte("fld1", left), lte("fld1", right));
            });
        }

        for (String idxName: F.asList(IDX_FLD1_FLD2, null)) {
            withClientCache((cache) -> {
                // Multiple field, multiple criteria.
                assertClientQuery(cache, left + 1, right, idxName, gt("fld1", left), lt("fld2", right));
            });
        }
    }

    /** */
    @Test
    public void testIndexNameMismatchCriteria() {
        withClientCache((cache) -> {
            for (IndexQueryCriterion[] criteria: F.asList(
                new IndexQueryCriterion[] { lt("fld1", 100), lt("fld2", 100) },
                new IndexQueryCriterion[] { lt("fld2", 100) }
            )) {
                IndexQuery<Integer, Person> idxQry = new IndexQuery<Integer, Person>(Person.class, IDX_FLD1)
                    .setCriteria(criteria);

                GridTestUtils.assertThrows(
                    log,
                    () -> cache.query(idxQry).getAll(),
                    ClientException.class,
                    "Failed to parse IndexQuery. Index doesn't match criteria");
            }
        });
    }

    /** */
    @Test
    public void testPageSize() {
        IndexQuery<Integer, Person> idxQry = new IndexQuery<>(Person.class);

        withClientCache(cache -> {
            for (int pageSize: F.asList(1, 10, 100, 1000, 10_000)) {
                idxQry.setPageSize(pageSize);

                TestRecordingCommunicationSpi.spi(grid(0)).record(GridQueryNextPageRequest.class);

                assertClientQuery(cache, 0, CNT, null);

                List<Object> reqs = TestRecordingCommunicationSpi.spi(grid(0)).recordedMessages(true);

                for (Object r: reqs)
                    assertEquals(pageSize, ((GridQueryNextPageRequest)r).pageSize());
            }

            for (int pageSize: F.asList(-10, -1, 0)) {
                GridTestUtils.assertThrowsAnyCause(
                    log,
                    () -> idxQry.setPageSize(pageSize),
                    IllegalArgumentException.class,
                    "Page size must be above zero");
            }
        });
    }

    /** */
    @Test
    public void testLocal() {
        withClientCache(cache -> {
            IndexQuery<Integer, Person> idxQry = new IndexQuery<>(Person.class);

            idxQry.setLocal(true);

            TestRecordingCommunicationSpi.spi(grid(0)).record(GridQueryNextPageRequest.class);

            assertTrue(cache.query(idxQry).getAll().size() < CNT);

            List<Object> reqs = TestRecordingCommunicationSpi.spi(grid(0)).recordedMessages(true);

            assertTrue(reqs.isEmpty());
        });
    }

    /** */
    @Test
    public void testFilter() {
        IndexQuery idxQry = new IndexQuery(Person.class);
        idxQry.setFilter((k, v) -> (int)k < 1000);

        withClientCache((cache) -> assertClientQuery(cache, 0, 1000, idxQry));
    }

    /** */
    @Test
    public void testPartition() {
        withClientCache(cache -> {
            IndexQuery<Integer, Person> idxQry = new IndexQuery<>(Person.class);

            for (int p = 0; p < 1024; p++) {
                idxQry.setPartition(p);

                for (int i = 0; i < NODES; i++)
                    TestRecordingCommunicationSpi.spi(grid(i)).record(GridQueryNextPageRequest.class);

                List<Cache.Entry<Integer, Person>> result = cache.query(idxQry).getAll();

                assertTrue(result.size() < CNT);

                for (Cache.Entry<Integer, Person> e: result)
                    assertEquals(p, grid(0).affinity("CACHE").partition(e.getKey()));

                for (int i = 0; i < NODES; i++) {
                    List<Object> reqs = TestRecordingCommunicationSpi.spi(grid(0)).recordedMessages(true);

                    assertTrue(reqs.isEmpty());
                }
            }

            for (int part: F.asList(-10, -1)) {
                GridTestUtils.assertThrows(
                    log,
                    () -> idxQry.setPartition(part),
                    IllegalArgumentException.class,
                    "Specified partition must be in the range");
            }

            GridTestUtils.assertThrows(
                log,
                () -> {
                    idxQry.setPartition(5000);

                    return cache.query(idxQry).getAll();
                },
                ClientException.class,
                "Specified partition must be in the range");
        });
    }

    /** */
    @Test
    public void testWrongIndexQueryCriterion() {
        withClientCache(cache -> {
            IndexQuery<Integer, Person> idxQry = new IndexQuery<>(Person.class);
            idxQry.setCriteria(new IndexQueryCriterion() {
                @Override public String field() {
                    return null;
                }
            });

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> cache.query(idxQry).getAll(),
                IllegalArgumentException.class,
                "Unknown IndexQuery criterion type");
        });
    }

    /** */
    private void assertClientQuery(
        ClientCache<Integer, Person> cache,
        int left,
        int right,
        @Nullable String idxName,
        IndexQueryCriterion... crit
    ) {
        IndexQuery<Integer, Person> idxQry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(crit);

        assertClientQuery(cache, left, right, idxQry);
    }

    /** */
    private void assertClientQuery(ClientCache<Integer, Person> cache, int left, int right, IndexQuery idxQry) {
        Iterator<Cache.Entry<Integer, Person>> cursor = cache.query(idxQry).iterator();

        for (int i = left; i < right; i++) {
            Cache.Entry<Integer, Person> e = cursor.next();

            assertEquals(i, e.getKey().intValue());

            if (keepBinary) {
                assertEquals(i, (int)((BinaryObject)e.getValue()).field("fld1"));
                assertEquals(i, (int)((BinaryObject)e.getValue()).field("fld2"));
            }
            else {
                assertEquals(i, e.getValue().fld1);
                assertEquals(i, e.getValue().fld2);
            }
        }
    }

    /** */
    private void withClientCache(Consumer<ClientCache<Integer, Person>> consumer) {
        ClientConfiguration clnCfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:10800");

        try (IgniteClient cln = Ignition.startClient(clnCfg)) {
            ClientCache<Integer, Person> cache = cln.cache("CACHE");

            if (keepBinary)
                cache = cache.withKeepBinary();

            consumer.accept(cache);
        }
    }

    /** */
    private static class Person {
        /** */
        @GridToStringInclude
        @QuerySqlField(orderedGroups = {
            @QuerySqlField.Group(name = IDX_FLD1, order = 0),
            @QuerySqlField.Group(name = IDX_FLD1_FLD2, order = 0)
        })
        final int fld1;

        /** */
        @GridToStringInclude
        @QuerySqlField(orderedGroups = {
            @QuerySqlField.Group(name = IDX_FLD1_FLD2, order = 1)
        })
        final int fld2;

        /** */
        Person(int fld1, int fld2) {
            this.fld1 = fld1;
            this.fld2 = fld2;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
