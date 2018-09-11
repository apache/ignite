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

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RetryException;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.JOIN_SQL;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Organization;
import static org.apache.ignite.internal.processors.query.h2.twostep.JoinSqlTestHelper.Person;

/**
 * Test for 6 retry cases
 */
public class RetryCauseMessageSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private static final String ORG_SQL = "select * from Organization";

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
    public void testSynthCacheWasNotFoundMessage() {
        GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec",
            new MockGridMapQueryExecutor(null) {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    if (GridH2QueryRequest.class.isAssignableFrom(msg.getClass())) {
                        GridH2QueryRequest qryReq = (GridH2QueryRequest)msg;

                        qryReq.caches().add(Integer.MAX_VALUE);

                        startedExecutor.onMessage(nodeId, msg);

                        qryReq.caches().remove(qryReq.caches().size() - 1);
                    }
                    else
                        startedExecutor.onMessage(nodeId, msg);
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
            GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to reserve partitions for query (group reservation failed)
     */
    public void testGrpReservationFailureMessage() {
        final GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final ConcurrentMap<MapReservationKey, GridReservable> reservations = GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "reservations");

        GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec",
            new MockGridMapQueryExecutor(null) {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    if (GridH2QueryRequest.class.isAssignableFrom(msg.getClass())) {
                        final MapReservationKey grpKey = new MapReservationKey(ORG, null);

                        reservations.put(grpKey, new GridReservable() {

                            @Override public boolean reserve() {
                                return false;
                            }

                            @Override public void release() {}
                        });
                    }
                    startedExecutor.onMessage(nodeId, msg);
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
            GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to reserve partitions for query (partition of REPLICATED cache is not in OWNING state)
     */
    public void testReplicatedCacheReserveFailureMessage() {
        GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final GridKernalContext ctx = GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "ctx");

        GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec",
            new MockGridMapQueryExecutor(null) {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    if (GridH2QueryRequest.class.isAssignableFrom(msg.getClass())) {
                        GridH2QueryRequest qryReq = (GridH2QueryRequest)msg;

                        GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(qryReq.caches().get(0));

                        GridDhtLocalPartition part = cctx.topology().localPartition(0, NONE, false);

                        AtomicLong aState = GridTestUtils.getFieldValue(part, GridDhtLocalPartition.class, "state");

                        long stateVal = aState.getAndSet(2);

                        startedExecutor.onMessage(nodeId, msg);

                        aState.getAndSet(stateVal);
                    }
                    else 
                        startedExecutor.onMessage(nodeId, msg);
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
            GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to reserve partitions for query (partition of PARTITIONED cache cannot be reserved)
     */
    public void testPartitionedCacheReserveFailureMessage() {
        GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final GridKernalContext ctx = GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "ctx");

        GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec",
            new MockGridMapQueryExecutor(null) {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    if (GridH2QueryRequest.class.isAssignableFrom(msg.getClass())) {
                        GridH2QueryRequest qryReq = (GridH2QueryRequest)msg;

                        GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(qryReq.caches().get(0));

                        GridDhtLocalPartition part = cctx.topology().localPartition(0, NONE, false);

                        AtomicLong aState = GridTestUtils.getFieldValue(part, GridDhtLocalPartition.class, "state");

                        long stateVal = aState.getAndSet(2);

                        startedExecutor.onMessage(nodeId, msg);

                        aState.getAndSet(stateVal);
                    }
                    else
                        startedExecutor.onMessage(nodeId, msg);

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
            GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /**
     * Failed to execute non-collocated query (will retry)
     */
    public void testNonCollocatedFailureMessage() {
        final GridMapQueryExecutor mapQryExec = GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec");

        final ConcurrentMap<MapReservationKey, GridReservable> reservations = GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "reservations");

        GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec",
            new MockGridMapQueryExecutor(null) {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    if (GridH2QueryRequest.class.isAssignableFrom(msg.getClass())) {
                        final MapReservationKey grpKey = new MapReservationKey(ORG, null);

                        reservations.put(grpKey, new GridReservable() {

                            @Override public boolean reserve() {
                                throw new GridH2RetryException("test retry exception");
                            }

                            @Override public void release() {
                            }
                        });
                    }
                    startedExecutor.onMessage(nodeId, msg);

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
            GridTestUtils.setFieldValue(h2Idx, IgniteH2Indexing.class, "mapQryExec", mapQryExec);
        }
        fail();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi(){
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
        System.setProperty(IGNITE_SQL_RETRY_TIMEOUT, "5000");

        Ignite ignite = startGridsMultiThreaded(NODES_COUNT, false);

        GridQueryProcessor qryProc = grid(ignite.name()).context().query();

        h2Idx = GridTestUtils.getFieldValue(qryProc, GridQueryProcessor.class, "idx");

        personCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Person>("pers")
            .setIndexedTypes(String.class, Person.class)
        );

        orgCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Organization>(ORG)
            .setCacheMode(CacheMode.REPLICATED)
            .setIndexedTypes(String.class, Organization.class)
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
     * Wrapper around @{GridMapQueryExecutor}
     */
    private abstract static class MockGridMapQueryExecutor extends GridMapQueryExecutor {

        /**
         * Wrapped executor
         */
        GridMapQueryExecutor startedExecutor;

        /** */
        MockGridMapQueryExecutor insertRealExecutor(GridMapQueryExecutor realExecutor) {
            this.startedExecutor = realExecutor;
            return this;
        }

        /**
         * @param busyLock Busy lock.
         */
        MockGridMapQueryExecutor(GridSpinBusyLock busyLock) {
            super(busyLock);
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            startedExecutor.onMessage(nodeId, msg);
        }

        /** {@inheritDoc} */
        @Override public void cancelLazyWorkers() {
            startedExecutor.cancelLazyWorkers();
        }

        /** {@inheritDoc} */
        @Override GridSpinBusyLock busyLock() {
            return startedExecutor.busyLock();
        }

        /** {@inheritDoc} */
        @Override public void onCacheStop(String cacheName) {
            startedExecutor.onCacheStop(cacheName);
        }

        /** {@inheritDoc} */
        @Override public void stopAndUnregisterCurrentLazyWorker() {
            startedExecutor.stopAndUnregisterCurrentLazyWorker();
        }

        /** {@inheritDoc} */
        @Override public void unregisterLazyWorker(MapQueryLazyWorker worker) {
            startedExecutor.unregisterLazyWorker(worker);
        }

        /** {@inheritDoc} */
        @Override public int registeredLazyWorkers() {
            return startedExecutor.registeredLazyWorkers();
        }
    }

}
