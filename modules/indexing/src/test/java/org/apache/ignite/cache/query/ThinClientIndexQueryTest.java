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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
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
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.in;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
@RunWith(Parameterized.class)
public class ThinClientIndexQueryTest extends GridCommonAbstractTest {
    /** */
    private static final int CNT = 10_000;

    /** */
    private static final int NULLS_CNT = -10;

    /** */
    private static final int NODES = 2;

    /** */
    private static final String IDX_FLD1 = "IDX_FLD1";

    /** */
    private static final String IDX_FLD1_FLD2 = "IDX_FLD1_FLD2";

    /** */
    private static final String IDX_FLD3 = "IDX_FLD3";

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
                stream.addData(i, new Person(i, i, new IndexQueryAllTypesTest.PojoField(i)));

            for (int i = NULLS_CNT; i < 0; i++)
                stream.addData(i, new Person(null, null, null));
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
                assertClientQuery(cache, NULLS_CNT, CNT, idxName);

                // Single field, single criterion.
                assertClientQuery(cache, left + 1, CNT, idxName, gt("fld1", left));
                assertClientQuery(cache, left, CNT, idxName, gte("fld1", left));
                assertClientQuery(cache, NULLS_CNT, left, idxName, lt("fld1", left));
                assertClientQuery(cache, NULLS_CNT, left + 1, idxName, lte("fld1", left));
                assertClientQuery(cache, left, left + 1, idxName, eq("fld1", left));
                assertClientQuery(cache, left, right + 1, idxName, between("fld1", left, right));
                assertClientQuery(cache, left, left + 1, idxName, in("fld1", Collections.singleton(left)));

                // Single field, multiple criteria.
                assertClientQuery(cache, left, right + 1, idxName, gte("fld1", left), lte("fld1", right));
                assertClientQuery(cache, left + 1, right + 1, idxName, gt("fld1", left), lte("fld1", right));
                assertClientQuery(cache, left, right, idxName, gte("fld1", left), lt("fld1", right));
                assertClientQuery(cache, left + 1, right, idxName, gt("fld1", left), lt("fld1", right));
                assertClientQuery(cache, right, right + 1, idxName,
                    gte("fld1", left), in("fld1", Collections.singleton(right)));

                // Single field, with nulls.
                assertClientQuery(cache, NULLS_CNT, CNT, idxName, gte("fld1", null));
                assertClientQuery(cache, 0, CNT, idxName, gt("fld1", null));
                assertClientQuery(cache, 0, 0, idxName, lt("fld1", null));
                assertClientQuery(cache, NULLS_CNT, 0, idxName, lte("fld1", null));
                assertClientQuery(cache, NULLS_CNT, 0, idxName, in("fld1", Collections.singleton(null)));
            });
        }

        for (String idxName: F.asList(IDX_FLD1_FLD2, null)) {
            withClientCache((cache) -> {
                // Multiple field, multiple criteria.
                assertClientQuery(cache, left + 1, right, idxName, gt("fld1", left), lt("fld2", right));
                assertClientQuery(cache, right, right + 1, idxName,
                    gt("fld1", left), in("fld2", Collections.singleton(right)));

            });
        }
    }

    /** */
    @Test
    public void testPojoIndex() {
        withClientCache((cache) -> {
            Object pojo = new IndexQueryAllTypesTest.PojoField(100);

            for (IndexQueryCriterion[] criteria: F.asList(
                new IndexQueryCriterion[] { eq("fld3", pojo) },
                new IndexQueryCriterion[] { in("fld3", Collections.singleton(pojo)) })
            ) {
                IndexQuery<Integer, Person> idxQry = new IndexQuery<Integer, Person>(Person.class, IDX_FLD3)
                    .setCriteria(criteria);

                List<Cache.Entry<Integer, Person>> result = cache.query(idxQry).getAll();

                assertEquals(1, result.size());
                assertEquals(100, result.get(0).getKey().intValue());

                if (keepBinary)
                    assertEquals(pojo, ((BinaryObject)result.get(0).getValue()).field("fld3"));
                else
                    assertEquals(100, result.get(0).getKey().intValue());
            }
        });
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
                    "Failed to execute IndexQuery: Index doesn't match criteria");
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

                for (int i = 0; i < NODES; i++) {
                    TestRecordingCommunicationSpi.spi(grid(i)).record(
                        GridCacheQueryRequest.class,
                        GridCacheQueryResponse.class);
                }

                assertClientQuery(cache, NULLS_CNT, CNT, idxQry);

                int nodeOneEntries = cache.query(new ScanQuery<Integer, Person>().setLocal(true)).getAll().size();
                int nodeTwoEntries = (CNT - NULLS_CNT) - nodeOneEntries;

                int nodeOneExpectedReqs = (nodeOneEntries + pageSize - 1) / pageSize;
                int nodeTwoExpectedReqs = (nodeTwoEntries + pageSize - 1) / pageSize;

                int nodeOneLastPageEntries = nodeOneEntries % pageSize;
                int nodeTwoLastPageEntries = nodeTwoEntries % pageSize;

                List<Object> msgs = new ArrayList<>();

                for (int i = 0; i < NODES; i++)
                    msgs.addAll(TestRecordingCommunicationSpi.spi(grid(i)).recordedMessages(true));

                List<GridCacheQueryRequest> reqs = getFilteredMessages(msgs, GridCacheQueryRequest.class);
                List<GridCacheQueryResponse> resp = getFilteredMessages(msgs, GridCacheQueryResponse.class);

                int reqsSize = reqs.size();

                assert (reqsSize == nodeOneExpectedReqs || reqsSize == nodeTwoExpectedReqs) && reqsSize == resp.size();

                for (int i = 0; i < reqsSize; i++) {
                    int reqPage = reqs.get(i).pageSize();
                    int respData = resp.get(i).data().size();

                    assert reqPage == pageSize;

                    if (i == reqsSize - 1 && (nodeOneLastPageEntries != 0 || nodeTwoLastPageEntries != 0))
                        assert respData == nodeOneLastPageEntries || respData == nodeTwoLastPageEntries;
                    else
                        assert respData == reqPage;
                }
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
        idxQry.setFilter((k, v) -> (int)k >= 0 && (int)k < 1000);

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

        if (left < right) {
            Random r = new Random();

            int limit = 1 + r.nextInt(right - left);

            idxQry = new IndexQuery<Integer, Person>(Person.class, idxName)
                .setCriteria(crit)
                .setLimit(limit);

            assertClientQuery(cache, left, left + limit, idxQry);

            limit = right - left + r.nextInt(right - left);

            idxQry = new IndexQuery<Integer, Person>(Person.class, idxName)
                .setCriteria(crit)
                .setLimit(limit);

            assertClientQuery(cache, left, right, idxQry);
        }
    }

    /** */
    private void assertClientQuery(ClientCache<Integer, Person> cache, int left, int right, IndexQuery idxQry) {
        List<Cache.Entry<Integer, Person>> result = cache.query(idxQry).getAll();

        assertEquals(right - left, result.size());

        Function<Cache.Entry<Integer, Person>, Object> fldOneVal = (e) ->
            keepBinary ? ((BinaryObject)e.getValue()).field("fld1") : e.getValue().fld1;

        for (int i = 0; i < result.size(); i++) {
            Cache.Entry<Integer, Person> e = result.get(i);

            int key = left + i;

            if (key >= 0) {
                assertEquals(key, e.getKey().intValue());
                assertEquals(key, (int)fldOneVal.apply(e));
            }
            else
                assertEquals(null, fldOneVal.apply(e));
        }
    }

    /** */
    @Test
    public void testIndexQueryLimitOnOlderProtocolVersion() throws Exception {
        // Exclude INDEX_QUERY_LIMIT from protocol.
        Class<?> clazz = Class.forName("org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature");

        Field field = clazz.getDeclaredField("ALL_FEATURES_AS_ENUM_SET");

        field.setAccessible(true);

        EnumSet<ProtocolBitmaskFeature> allFeaturesEnumSet = (EnumSet<ProtocolBitmaskFeature>)field.get(null);

        allFeaturesEnumSet.remove(ProtocolBitmaskFeature.INDEX_QUERY_LIMIT);

        try {
            withClientCache((cache) -> {
                // No limit.
                IndexQuery<Integer, Person> idxQry = new IndexQuery<>(Person.class, IDX_FLD1);

                assertClientQuery(cache, NULLS_CNT, CNT, idxQry);

                // With limit.
                IndexQuery<Integer, Person> idxQryWithLImit = new IndexQuery<Integer, Person>(Person.class, IDX_FLD1)
                    .setLimit(10);

                GridTestUtils.assertThrowsAnyCause(
                    log,
                    () -> {
                        cache.query(idxQryWithLImit).getAll();
                        return null;
                    },
                    ClientFeatureNotSupportedByServerException.class,
                    "Feature INDEX_QUERY_LIMIT is not supported by the server");
            });
        }
        finally {
            //revert the features set
            allFeaturesEnumSet.add(ProtocolBitmaskFeature.INDEX_QUERY_LIMIT);
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

    /**
     * Filter messages in a collection by a certain class.
     *
     * @param msgs List of mixed messages.
     * @param cls Class of messages that need to be filtered.
     * @return List of messages filtered by the specified class.
     */
    public static <T> List<T> getFilteredMessages(List<Object> msgs, Class<T> cls) {
        return msgs.stream()
            .filter(msg -> msg.getClass().equals(cls))
            .map(msg -> (T)msg)
            .collect(Collectors.toList());
    }

    /** */
    private static class Person {
        /** */
        @GridToStringInclude
        @QuerySqlField(orderedGroups = {
            @QuerySqlField.Group(name = IDX_FLD1, order = 0),
            @QuerySqlField.Group(name = IDX_FLD1_FLD2, order = 0)
        })
        final Integer fld1;

        /** */
        @GridToStringInclude
        @QuerySqlField(orderedGroups = {
            @QuerySqlField.Group(name = IDX_FLD1_FLD2, order = 1)
        })
        final Integer fld2;

        /** */
        @QuerySqlField(orderedGroups = {
            @QuerySqlField.Group(name = IDX_FLD3, order = 0)
        })
        IndexQueryAllTypesTest.PojoField fld3;

        /** */
        Person(Integer fld1, Integer fld2, IndexQueryAllTypesTest.PojoField fld3) {
            this.fld1 = fld1;
            this.fld2 = fld2;
            this.fld3 = fld3;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
