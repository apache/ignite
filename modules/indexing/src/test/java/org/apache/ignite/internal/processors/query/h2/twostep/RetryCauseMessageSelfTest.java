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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.JOIN_SQL;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Organization;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Person;

/**
 * Test for 6 retry cases
 */
@WithSystemProperty(key = IGNITE_SQL_RETRY_TIMEOUT, value = "5000")
public class RetryCauseMessageSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private static final String ORG_SQL = "select * from Organization";

    /** */
    static final String UPDATE_SQL = "UPDATE Person SET name=lower(?) ";

    /** */
    private static final String ORG = "org";

    /** */
    private IgniteCache<String, Person> personCache;

    /** */
    private IgniteCache<String, Organization> orgCache;

    /** */
    private IgniteH2Indexing h2Idx;

    /** */
    @Override protected long getTestTimeout() {
        return 600 * 1000;
    }

    /**
     * Failed to reserve partitions for query (cache is not found on local node)
     */
    @Test
    public void testSynthCacheWasNotFoundMessage() {
        GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        GridTestUtils.setFieldValue(h2Idx, "mapQryExec",
            new MockGridMapQueryExecutor() {
                @Override public void onQueryRequest(ClusterNode node, GridH2QueryRequest qryReq)
                    throws IgniteCheckedException {
                    qryReq.caches().add(Integer.MAX_VALUE);

                    startedExecutor.onQueryRequest(node, qryReq);

                    qryReq.caches().remove(qryReq.caches().size() - 1);
                }
            }.insertRealExecutor(mapQryExec));

        SqlQuery<String, Person> qry = new SqlQuery<String, Person>(Person.class, JOIN_SQL).setArgs("Organization #0");

        qry.setDistributedJoins(true);

        try {
            personCache.query(qry).getAll();
        }
        catch (CacheException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Failed to reserve partitions for query (cache is not found on local node) ["));

            return;
        }
        finally {
            GridTestUtils.setFieldValue(h2Idx, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to reserve partitions for query (group reservation failed)
     */
    @Test
    public void testGrpReservationFailureMessage() {
        final GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final ConcurrentMap<PartitionReservationKey, GridReservable> reservations = reservations(h2Idx);

        GridTestUtils.setFieldValue(h2Idx, "mapQryExec",
            new MockGridMapQueryExecutor() {
                @Override public void onQueryRequest(ClusterNode node, GridH2QueryRequest qryReq)
                    throws IgniteCheckedException {
                    final PartitionReservationKey grpKey = new PartitionReservationKey(ORG, null);

                    reservations.put(grpKey, new GridReservable() {

                        @Override public boolean reserve() {
                            return false;
                        }

                        @Override public void release() {}
                    });
                    startedExecutor.onQueryRequest(node, qryReq);
                }
            }.insertRealExecutor(mapQryExec));

        SqlQuery<String, Person> qry = new SqlQuery<String, Person>(Person.class, JOIN_SQL).setArgs("Organization #0");

        qry.setDistributedJoins(true);

        try {
            personCache.query(qry).getAll();
        }
        catch (CacheException e) {
            assertTrue(e.getMessage().contains("Failed to reserve partitions for query (group reservation failed) ["));

            return;
        }
        finally {
            GridTestUtils.setFieldValue(h2Idx, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to reserve partitions for query (partition of REPLICATED cache is not in OWNING state)
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7039")
    @Test
    public void testReplicatedCacheReserveFailureMessage() {
        GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final GridKernalContext ctx = GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "ctx");

        GridTestUtils.setFieldValue(h2Idx, "mapQryExec",
            new MockGridMapQueryExecutor() {
                @Override public void onQueryRequest(ClusterNode node, GridH2QueryRequest qryReq) throws IgniteCheckedException {
                    GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(qryReq.caches().get(0));

                    GridDhtLocalPartition part = cctx.topology().localPartition(0, NONE, false);

                    AtomicLong aState = GridTestUtils.getFieldValue(part, GridDhtLocalPartition.class, "state");

                    long stateVal = aState.getAndSet(2);

                    startedExecutor.onQueryRequest(node, qryReq);

                    aState.getAndSet(stateVal);
                }
            }.insertRealExecutor(mapQryExec));

        SqlQuery<String, Organization> qry = new SqlQuery<>(Organization.class, ORG_SQL);

        qry.setDistributedJoins(true);

        try {
            orgCache.query(qry).getAll();
        }
        catch (CacheException e) {
            assertTrue(e.getMessage().contains("Failed to reserve partitions for query (partition of REPLICATED cache is not in OWNING state) ["));

            return;
        }
        finally {
            GridTestUtils.setFieldValue(h2Idx, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to reserve partitions for query (partition of PARTITIONED cache cannot be reserved)
     */
    @Test
    public void testPartitionedCacheReserveFailureMessage() {
        GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final GridKernalContext ctx = GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "ctx");

        GridTestUtils.setFieldValue(h2Idx, "mapQryExec",
            new MockGridMapQueryExecutor() {
                @Override public void onQueryRequest(ClusterNode node, GridH2QueryRequest qryReq)
                    throws IgniteCheckedException {
                    GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(qryReq.caches().get(0));

                    GridDhtLocalPartition part = cctx.topology().localPartition(0, NONE, false);

                    AtomicLong aState = GridTestUtils.getFieldValue(part, GridDhtLocalPartition.class, "state");

                    long stateVal = aState.getAndSet(2);

                    startedExecutor.onQueryRequest(node, qryReq);

                    aState.getAndSet(stateVal);
                }
            }.insertRealExecutor(mapQryExec));

        SqlQuery<String, Person> qry = new SqlQuery<String, Person>(Person.class, JOIN_SQL).setArgs("Organization #0");

        qry.setDistributedJoins(true);
        try {
            personCache.query(qry).getAll();
        }
        catch (CacheException e) {
            assertTrue(e.getMessage().contains("Failed to reserve partitions for query (partition of PARTITIONED " +
                "cache is not found or not in OWNING state) "));

            return;
        }
        finally {
            GridTestUtils.setFieldValue(h2Idx, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to execute non-collocated query (will retry)
     */
    @Test
    public void testNonCollocatedFailureMessage() {
        final GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final ConcurrentMap<PartitionReservationKey, GridReservable> reservations = reservations(h2Idx);

        GridTestUtils.setFieldValue(h2Idx, "mapQryExec",
            new MockGridMapQueryExecutor() {
                @Override public void onQueryRequest(ClusterNode node, GridH2QueryRequest qryReq)
                    throws IgniteCheckedException {
                    final PartitionReservationKey grpKey = new PartitionReservationKey(ORG, null);

                    reservations.put(grpKey, new GridReservable() {
                        @Override public boolean reserve() {
                            throw H2Utils.retryException("test retry exception");
                        }

                        @Override public void release() {}
                    });

                    startedExecutor.onQueryRequest(node, qryReq);
                }
            }.insertRealExecutor(mapQryExec));

        SqlQuery<String, Person> qry = new SqlQuery<String, Person>(Person.class, JOIN_SQL).setArgs("Organization #0");

        qry.setDistributedJoins(true);
        try {
            personCache.query(qry).getAll();
        }
        catch (CacheException e) {
            assertTrue(e.getMessage().contains("Failed to execute non-collocated query (will retry) ["));

            return;
        }
        finally {
            GridTestUtils.setFieldValue(h2Idx, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Test query remap failure reason.
     */
    @Test
    public void testQueryMappingFailureMessage() {
        final GridReduceQueryExecutor rdcQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "rdcQryExec");
        final ReducePartitionMapper mapper = GridTestUtils.getFieldValue(rdcQryExec, GridReduceQueryExecutor.class, "mapper");

        final IgniteLogger logger = GridTestUtils.getFieldValue(rdcQryExec, GridReduceQueryExecutor.class, "log");
        final GridKernalContext ctx = GridTestUtils.getFieldValue(rdcQryExec, GridReduceQueryExecutor.class, "ctx");

        GridTestUtils.setFieldValue(rdcQryExec, "mapper",
            new ReducePartitionMapper(ctx, logger) {
                @Override public ReducePartitionMapResult nodesForPartitions(List<Integer> cacheIds,
                    AffinityTopologyVersion topVer, int[] parts, boolean isReplicatedOnly) {
                    final ReducePartitionMapResult res = super.nodesForPartitions(cacheIds, topVer, parts, isReplicatedOnly);

                    return new ReducePartitionMapResult(Collections.emptyList(), res.partitionsMap(), res.queryPartitionsMap());
                }
            });

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery(JOIN_SQL).setArgs("Organization #0");

            final Throwable throwable = GridTestUtils.assertThrows(log, () -> {
                return personCache.query(qry).getAll();
            }, CacheException.class, "Failed to map SQL query to topology during timeout:");

            throwable.printStackTrace();
        }
        finally {
            GridTestUtils.setFieldValue(rdcQryExec, "mapper", mapper);
        }
    }

    /**
     * Test update query remap failure reason.
     */
    @Test
    public void testUpdateQueryMappingFailureMessage() {
        final GridReduceQueryExecutor rdcQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "rdcQryExec");
        final ReducePartitionMapper mapper = GridTestUtils.getFieldValue(rdcQryExec, GridReduceQueryExecutor.class, "mapper");

        final IgniteLogger logger = GridTestUtils.getFieldValue(rdcQryExec, GridReduceQueryExecutor.class, "log");
        final GridKernalContext ctx = GridTestUtils.getFieldValue(rdcQryExec, GridReduceQueryExecutor.class, "ctx");

        GridTestUtils.setFieldValue(rdcQryExec, "mapper",
            new ReducePartitionMapper(ctx, logger) {
                @Override public ReducePartitionMapResult nodesForPartitions(List<Integer> cacheIds,
                    AffinityTopologyVersion topVer, int[] parts, boolean isReplicatedOnly) {
                    final ReducePartitionMapResult res = super.nodesForPartitions(cacheIds, topVer, parts, isReplicatedOnly);

                    return new ReducePartitionMapResult(Collections.emptyList(), res.partitionsMap(), res.queryPartitionsMap());
                }
            });

        try {
            final SqlFieldsQueryEx qry = new SqlFieldsQueryEx(UPDATE_SQL, false)
                .setArgs("New Name");

            GridTestUtils.assertThrows(log, () -> {
                return personCache.query(qry).getAll();
            }, CacheException.class, "Failed to map SQL query to topology during timeout");

            qry.setArgs("Another Name");
            qry.setSkipReducerOnUpdate(true);

            GridTestUtils.assertThrows(log, () -> {
                return personCache.query(qry).getAll();
            }, CacheException.class, "Failed to determine nodes participating in the update. ");
        }
        finally {
            GridTestUtils.setFieldValue(rdcQryExec, "mapper", mapper);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                assert msg != null;

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ignite = startGridsMultiThreaded(NODES_COUNT, false);

        GridQueryProcessor qryProc = grid(ignite.name()).context().query();

        h2Idx = GridTestUtils.getFieldValue(qryProc, GridQueryProcessor.class, "idx");

        personCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Person>("pers")
            .setQueryEntities(JoinSqlTestHelper.personQueryEntity())
        );

        orgCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Organization>(ORG)
            .setCacheMode(CacheMode.REPLICATED)
            .setQueryEntities(JoinSqlTestHelper.organizationQueryEntity())
        );

        awaitPartitionMapExchange();

        JoinSqlTestHelper.populateDataIntoOrg(orgCache);

        JoinSqlTestHelper.populateDataIntoPerson(personCache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param h2Idx Indexing.
     * @return Current reservations.
     */
    private static ConcurrentMap<PartitionReservationKey, GridReservable> reservations(IgniteH2Indexing h2Idx) {
        PartitionReservationManager partReservationMgr = h2Idx.partitionReservationManager();

        return GridTestUtils.getFieldValue(partReservationMgr, PartitionReservationManager.class, "reservations");
    }

    /**
     * Wrapper around @{GridMapQueryExecutor}
     */
    private abstract static class MockGridMapQueryExecutor extends GridMapQueryExecutor {
        /** Wrapped executor */
        GridMapQueryExecutor startedExecutor;

        /**
         * @param startedExecutor Started executor.
         * @return Mocked map query executor.
         */
        MockGridMapQueryExecutor insertRealExecutor(GridMapQueryExecutor startedExecutor) {
            this.startedExecutor = startedExecutor;

            return this;
        }

        /** {@inheritDoc} */
        @Override public void onQueryRequest(ClusterNode node, GridH2QueryRequest req) throws IgniteCheckedException {
            startedExecutor.onQueryRequest(node, req);
        }
    }
}
