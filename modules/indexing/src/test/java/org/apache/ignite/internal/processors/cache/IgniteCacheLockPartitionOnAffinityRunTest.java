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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-2310
 */
public class IgniteCacheLockPartitionOnAffinityRunTest extends IgniteCacheLockPartitionOnAffinityRunAbstractTest {
    /** Flag to use custom CollisionSpi that cancels all jobs. */
    private static boolean collisionSpiCancelAll;

    /**
     * @param ignite Ignite.
     * @param orgId Org id.
     * @param expectedReservations Expected reservations.
     */
    private static void checkPartitionsReservations(final IgniteEx ignite, int orgId,
        int expectedReservations) {
        int part = ignite.affinity(Organization.class.getSimpleName()).partition(orgId);

        GridDhtLocalPartition pPers = ignite.context().cache()
            .internalCache(Person.class.getSimpleName()).context().topology()
            .localPartition(part, AffinityTopologyVersion.NONE, false);

        GridDhtLocalPartition pOrgs = ignite.context().cache()
            .internalCache(Organization.class.getSimpleName()).context().topology()
            .localPartition(part, AffinityTopologyVersion.NONE, false);

        assertEquals("Unexpected reservations count", expectedReservations, pOrgs.reservations());
        assertEquals("Unexpected reservations count", expectedReservations, pPers.reservations());
    }

    /**
     * @param ignite Ignite.
     * @param log Logger.
     * @param orgId Organization id.
     * @return Count of found Person object with specified orgId
     */
    private static int getPersonsCountFromPartitionMapCheckBothCaches(final IgniteEx ignite, IgniteLogger log,
        int orgId) {
        int part = ignite.affinity(Organization.class.getSimpleName()).partition(orgId);

        GridCacheAdapter<?, ?> cacheAdapterOrg = ignite.context().cache()
            .internalCache(Organization.class.getSimpleName());
        GridCacheAdapter<?, ?> cacheAdapterPers = ignite.context().cache()
            .internalCache(Person.class.getSimpleName());

        if (cacheAdapterOrg != null && cacheAdapterPers != null &&
            cacheAdapterOrg.context() != null && cacheAdapterPers.context() != null) {
            GridDhtLocalPartition pPers = cacheAdapterPers.context().topology()
                .localPartition(part, AffinityTopologyVersion.NONE, false);

            int cnt = 0;
            for (GridCacheMapEntry e : pPers.entries()) {
                Person.Key k = (Person.Key)e.keyValue(false);
                Person p = null;
                try {
                    p = e.val.value(ignite.context().cacheObjects().contextForCache(
                        cacheAdapterPers.cacheCfg), false);
                }
                catch (IgniteCheckedException e1) {
                    // No-op.
                }

                if (p != null && p.getOrgId() == orgId && k.orgId == orgId)
                    ++cnt;
            }

            return cnt;
        }
        else
            fail("There is no cache");

        return -1;
    }

    /**
     * @param ignite Ignite.
     * @param log Logger.
     * @param orgId Organization id.
     * @return Count of found Person object with specified orgId
     */
    private static int getPersonsCountFromPartitionMap(final IgniteEx ignite, IgniteLogger log, int orgId) {
        int part = ignite.affinity(Organization.class.getSimpleName()).partition(orgId);

        GridCacheAdapter<?, ?> cacheAdapterPers = ignite.context().cache()
            .internalCache(Person.class.getSimpleName());

        if (cacheAdapterPers != null && cacheAdapterPers.context() != null) {
            GridDhtLocalPartition pPers = cacheAdapterPers.context().topology()
                .localPartition(part, AffinityTopologyVersion.NONE, false);

            int cnt = 0;
            for (GridCacheMapEntry e : pPers.entries()) {
                Person.Key k = (Person.Key)e.keyValue(false);
                Person p = null;
                try {
                    p = e.val.value(ignite.context().cacheObjects().contextForCache(
                        cacheAdapterPers.cacheCfg), false);
                }
                catch (IgniteCheckedException e1) {
                    // No-op.
                }

                if (p != null && p.getOrgId() == orgId && k.orgId == orgId)
                    ++cnt;
            }

            return cnt;
        }
        else
            fail("There is no cache");

        return -1;
    }

    /**
     * @param ignite Ignite.
     * @param log Logger.
     * @param orgId Organization id.
     * @return Count of found Person object with specified orgId
     */
    private static int getPersonsCountBySqlFieledLocalQuery(final IgniteEx ignite, IgniteLogger log, int orgId) {
        List res = ignite.cache(Person.class.getSimpleName())
            .query(new SqlFieldsQuery(
                String.format("SELECT p.id FROM \"%s\".Person as p " +
                        "WHERE p.orgId = " + orgId,
                    Person.class.getSimpleName())).setLocal(true))
            .getAll();

        return res.size();
    }

    /**
     * @param ignite Ignite.
     * @param log Logger.
     * @param orgId Organization id.
     * @return Count of found Person object with specified orgId
     */
    private static int getPersonsCountBySqlFieledLocalQueryJoinOrgs(final IgniteEx ignite, IgniteLogger log,
        int orgId) {
        List res = ignite.cache(Person.class.getSimpleName())
            .query(new SqlFieldsQuery(
                String.format("SELECT p.id FROM \"%s\".Person as p, \"%s\".Organization as o " +
                        "WHERE p.orgId = o.id " +
                        "AND p.orgId = " + orgId,
                    Person.class.getSimpleName(),
                    Organization.class.getSimpleName())).setLocal(true))
            .getAll();

        return res.size();
    }

    /**
     * @param ignite Ignite.
     * @param log Logger.
     * @param orgId Organization id.
     * @return Count of found Person object with specified orgId
     */
    private static int getPersonsCountBySqlLocalQuery(final IgniteEx ignite, IgniteLogger log, int orgId) {
        List res = ignite.cache(Person.class.getSimpleName())
            .query(new SqlQuery<Person.Key, Person>(Person.class, "orgId = ?").setArgs(orgId).setLocal(true))
            .getAll();

        return res.size();
    }

    /**
     * @param ignite Ignite.
     * @param log Logger.
     * @param orgId Organization id.
     * @return Count of found Person object with specified orgId
     */
    private static int getPersonsCountByScanLocalQuery(final IgniteEx ignite, IgniteLogger log, final int orgId) {
        List res = ignite.cache(Person.class.getSimpleName())
            .query(new ScanQuery<>(new IgniteBiPredicate<Person.Key, Person>() {
                @Override public boolean apply(Person.Key key, Person person) {
                    return person.getOrgId() == orgId;
                }
            }).setLocal(true)).getAll();

        return res.size();
    }

    /**
     * @param personsCountGetter Interface to calculate count Person objects
     * @throws Exception If failed.
     */
    private void affinityRunWithoutExtraCaches(final PersonsCountGetter personsCountGetter) throws Exception {
        // Workaround for initial update job metadata.
        grid(0).compute().affinityRun(Organization.class.getSimpleName(), orgIds.get(0),
            new TestAffinityRun(personsCountGetter, orgIds.get(0)));

        // Run restart threads: start re-balancing
        beginNodesRestart();

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (System.currentTimeMillis() < endTime) {
                        for (final int orgId : orgIds) {
                            if (System.currentTimeMillis() >= endTime)
                                break;

                            grid(0).compute().affinityRun(Person.class.getSimpleName(), new Person(0, orgId).createKey(),
                                new TestAffinityRun(personsCountGetter, orgId));
                        }
                    }
                }
            }, AFFINITY_THREADS_COUNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();
        }
    }

    /**
     * @param personsCountGetter Persons count getter interface
     * @throws Exception If failed.
     */
    private void affinityRunWithExtraCaches(final PersonsCountGetter personsCountGetter) throws Exception {
        // Workaround for initial update job metadata.
        grid(0).compute().affinityRun(new TestAffinityRun(personsCountGetter, orgIds.get(0)),
            Arrays.asList(Person.class.getSimpleName(), Organization.class.getSimpleName()),
            orgIds.get(0));

        // Run restart threads: start re-balancing
        beginNodesRestart();

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (System.currentTimeMillis() < endTime) {
                        for (final int orgId : orgIds) {
                            if (System.currentTimeMillis() >= endTime)
                                break;

                            grid(0).compute().affinityRun(new TestAffinityRun(personsCountGetter, orgId),
                                Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()),
                                orgId);
                        }
                    }
                }
            }, AFFINITY_THREADS_COUNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();
        }
    }

    /**
     * @param personsCountGetter Persons count getter interface
     * @throws Exception If failed.
     */
    private void affinityCallWithoutExtraCaches(final PersonsCountGetter personsCountGetter) throws Exception {
        // Workaround for initial update job metadata.
        grid(0).compute().affinityCall(Person.class.getSimpleName(),
            new Person(0, orgIds.get(0)).createKey(),
            new TestAffinityCall(personsCountGetter, orgIds.get(0)));

        // Run restart threads: start re-balancing
        beginNodesRestart();

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (System.currentTimeMillis() < endTime) {
                        for (final int orgId : orgIds) {
                            if (System.currentTimeMillis() >= endTime)
                                break;

                            int personsCnt = grid(0).compute().affinityCall(Person.class.getSimpleName(),
                                new Person(0, orgId).createKey(),
                                new TestAffinityCall(personsCountGetter, orgId));
                            assertEquals(PERS_AT_ORG_COUNT, personsCnt);
                        }
                    }
                }
            }, AFFINITY_THREADS_COUNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();
        }
    }

    /**
     * @param personsCountGetter Persons count getter interface
     * @throws Exception If failed.
     */
    private void affinityCallWithExtraCaches(final PersonsCountGetter personsCountGetter) throws Exception {
        // Workaround for initial update job metadata.
        grid(0).compute().affinityCall(new TestAffinityCall(personsCountGetter, orgIds.get(0)),
            Arrays.asList(Person.class.getSimpleName(), Organization.class.getSimpleName()),
            orgIds.get(0));

        // Run restart threads: start re-balancing
        beginNodesRestart();

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (System.currentTimeMillis() < endTime) {
                        for (final int orgId : orgIds) {
                            if (System.currentTimeMillis() >= endTime)
                                break;

                            int personsCnt = grid(0).compute().affinityCall(new TestAffinityCall(personsCountGetter, orgId),
                                Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()),
                                orgId);

                            assertEquals(PERS_AT_ORG_COUNT, personsCnt);
                        }
                    }
                }
            }, AFFINITY_THREADS_COUNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunCheckDhtPartition() throws Exception {
        affinityRunWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountFromPartitionMap(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunWithCachesCheckDhtPartition() throws Exception {
        affinityRunWithExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountFromPartitionMapCheckBothCaches(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunCheckSqlFieldLocalQuery() throws Exception {
        affinityRunWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlFieledLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunWithCachesCheckSqlFieldLocalQuery() throws Exception {
        affinityRunWithExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlFieledLocalQueryJoinOrgs(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunCheckSqlLocalQuery() throws Exception {
        affinityRunWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunWithCachesCheckSqlLocalQuery() throws Exception {
        affinityRunWithExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunCheckScanLocalQuery() throws Exception {
        affinityRunWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountByScanLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCheckDhtPartition() throws Exception {
        affinityCallWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountFromPartitionMap(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallWithCachesCheckDhtPartition() throws Exception {
        affinityCallWithExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountFromPartitionMap(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCheckSqlFieldLocalQuery() throws Exception {
        affinityCallWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlFieledLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallWithCachesCheckSqlFieldLocalQuery() throws Exception {
        affinityCallWithExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlFieledLocalQueryJoinOrgs(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCheckSqlLocalQuery() throws Exception {
        affinityCallWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallWithCachesCheckSqlLocalQuery() throws Exception {
        affinityCallWithExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountBySqlLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallCheckScanLocalQuery() throws Exception {
        affinityCallWithoutExtraCaches(new PersonsCountGetter() {
            @Override public int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId) {
                return getPersonsCountByScanLocalQuery(ignite, log, orgId);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCheckReservePartitionException() throws Exception {

        int orgId = 0;
        for (Integer orgId1 : orgIds) {
            orgId = orgId1;
            ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);
            if (!grid(0).localNode().id().equals(node.id()))
                break;
        }

        try {
            grid(0).compute().affinityRun(new IgniteRunnable() {
                @Override public void run() {
                    // No-op.
                }
            }, Arrays.asList(Organization.class.getSimpleName(), OTHER_CACHE_NAME), orgId);

            fail("Exception is expected");
        }
        catch (Exception e) {
            assertTrue(EXCEPTION_MSG_REGEX.matcher(e.getCause().getMessage()).find());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobCompletesNormally() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        grid(0).compute().affinityRun(new IgniteRunnable() {
            @Override public void run() {
                checkPartitionsReservations((IgniteEx)grid(node), orgId, 1);
            }
        }, Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);

        checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobThrowsException() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        try {
            grid(0).compute().affinityRun(new IgniteRunnable() {
                @Override public void run() {
                    checkPartitionsReservations((IgniteEx)grid(node), orgId, 1);
                    throw new RuntimeException("Test job throws exception");
                }
            }, Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);
            fail("Exception must be thrown");
        }
        catch (Exception e) {
            checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobThrowsError() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        try {
            grid(1).compute().affinityRun(new IgniteRunnable() {
                @Override public void run() {
                    checkPartitionsReservations((IgniteEx)grid(node), orgId, 1);
                    throw new Error("Test job throws error");
                }
            }, Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);
            fail("Error must be thrown");
        }
        catch (Throwable e) {
            checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobUnmarshalingFails() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        // Test job unmarshaling fails
        try {
            grid(1).compute().affinityRun(new JobFailUnmarshaling(),
                Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);
            fail("Unmarshaling exception must be thrown");
        }
        catch (Exception e) {
            checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobCanceledByCollisionSpi() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        try {
            collisionSpiCancelAll = true;

            grid(1).compute().affinityRun(new IgniteRunnable() {
                @Override public void run() {
                    fail("Must not be executed");
                }
            }, Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);
            fail("Error must be thrown");
        }
        catch (ClusterTopologyException e) {
            checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
        }
        finally {
            collisionSpiCancelAll = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobMasterLeave() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        try {
            grid(1).compute().withAsync().affinityRun(new IgniteRunnable() {
                @Override public void run() {
                    checkPartitionsReservations((IgniteEx)grid(node), orgId, 1);
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        // No-op.
                    }
                }
            }, Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);

            stopGrid(1, true);
            Thread.sleep(3000);

            waitForRebalancing((IgniteEx)grid(node),
                ((IgniteEx)grid(node)).context().cache().context().exchange().topologyVersion());

            checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
        }
        finally {
            startGrid(1);
            awaitPartitionMapExchange();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobImplementMasterLeave() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        try {
            grid(1).compute().withAsync().affinityRun(new RunnableWithMasterLeave() {
                @Override public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteException {
                }

                @Override public void run() {
                    checkPartitionsReservations((IgniteEx)grid(node), orgId, 1);
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        // No-op.
                    }
                }
            }, Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);

            stopGrid(1, true);
            Thread.sleep(3000);

            waitForRebalancing((IgniteEx)grid(node),
                ((IgniteEx)grid(node)).context().cache().context().exchange().topologyVersion());

            checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
        }
        finally {
            startGrid(1);
            awaitPartitionMapExchange();
        }
    }

    /**
     * @param ignite Ignite.
     * @param top Topology.
     * @throws IgniteCheckedException If failed
     */
    protected void waitForRebalancing(IgniteEx ignite, AffinityTopologyVersion top) throws IgniteCheckedException {
        boolean finished = false;

        while (!finished) {
            finished = true;

            for (GridCacheAdapter c : ignite.context().cache().internalCaches()) {
                GridDhtPartitionDemander.RebalanceFuture fut = (GridDhtPartitionDemander.RebalanceFuture)c.preloader().rebalanceFuture();
                if (fut.topologyVersion() == null || !fut.topologyVersion().equals(top)) {
                    finished = false;

                    break;
                }
                else if (!fut.get()) {
                    finished = false;

                    log.warning("Rebalancing finished with missed partitions.");
                }
            }
        }
    }

    /** */
    private interface PersonsCountGetter {
        /**
         * @param ignite Ignite.
         * @param log Logger.
         * @param orgId Org id.
         * @return Count of found Person object with specified orgId
         */
        int getPersonsCount(IgniteEx ignite, IgniteLogger log, int orgId);
    }

    /** */
    interface RunnableWithMasterLeave extends IgniteRunnable, ComputeJobMasterLeaveAware {
    }

    /** */
    private static class TestAffinityCall implements IgniteCallable<Integer> {
        /** Persons count getter. */
        PersonsCountGetter personsCountGetter;

        /** Org id. */
        int orgId;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        public TestAffinityCall() {
            // No-op.
        }

        /**
         * @param personsCountGetter Object to count Person.
         * @param orgId Organization Id.
         */
        public TestAffinityCall(PersonsCountGetter personsCountGetter, int orgId) {
            this.personsCountGetter = personsCountGetter;
            this.orgId = orgId;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            log.info("Begin call. orgId " + orgId);
            return personsCountGetter.getPersonsCount(ignite, log, orgId);
        }
    }

    /** */
    private static class TestAffinityRun implements IgniteRunnable {
        /** Persons count getter. */
        PersonsCountGetter personsCountGetter;

        /** Org id. */
        int orgId;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        public TestAffinityRun() {
            // No-op.
        }

        /**
         * @param personsCountGetter Object to count Person.
         * @param orgId Organization Id.
         */
        public TestAffinityRun(PersonsCountGetter personsCountGetter, int orgId) {
            this.personsCountGetter = personsCountGetter;
            this.orgId = orgId;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            log.info("Begin run");
            int cnt = personsCountGetter.getPersonsCount(ignite, log, orgId);
            assertEquals(PERS_AT_ORG_COUNT, cnt);
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    @IgniteSpiMultipleInstancesSupport(true)
    public static class TestCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** Grid logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {

            if (!collisionSpiCancelAll) {
                Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

                for (CollisionJobContext job : waitJobs)
                    job.activate();
            }
            else {
                Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

                for (CollisionJobContext job : waitJobs)
                    job.cancel();
            }
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException {
            // Start SPI start stopwatch.
            startStopwatch();
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }
    }

    /** */
    static class JobFailUnmarshaling implements Externalizable, IgniteRunnable {

        /**
         * Default constructor (required by Externalizable).
         */
        public JobFailUnmarshaling() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            //No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new IOException("Test job unmarshaling fails");
        }

        /** {@inheritDoc} */
        @Override public void run() {
            fail("Must not be executed");
        }
    }

}