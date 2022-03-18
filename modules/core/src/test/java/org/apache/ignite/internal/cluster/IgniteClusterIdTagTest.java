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

package org.apache.ignite.internal.cluster;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.ClusterTagUpdatedEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.metastorage.persistence.DmsDataWriterWorker;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for ID and tag features of IgniteCluster.
 */
public class IgniteClusterIdTagTest extends GridCommonAbstractTest {
    /** */
    private static final String CUSTOM_TAG_0 = "my_super_cluster";

    /** */
    private static final String CUSTOM_TAG_1 = "not_so_super_but_OK";

    /** */
    private static final String CLIENT_CUSTOM_TAG_0 = "client_custom_tag_0";

    /** */
    private static final String CLIENT_CUSTOM_TAG_1 = "client_custom_tag_1";

    /** */
    private boolean isPersistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);
        else {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setInitialSize(128 * 1024 * 1024)
                        .setMaxSize(128 * 1024 * 1024)
                        .setPersistenceEnabled(isPersistenceEnabled)
                );

            cfg.setDataStorageConfiguration(dsCfg);
        }

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test verifies that cluster ID is generated upon cluster start
     * and correctly spread across all nodes joining later.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryClusterId() throws Exception {
        Ignite ig0 = startGrid(0);

        UUID id0 = ig0.cluster().id();

        assertNotNull(id0);

        Ignite ig1 = startGrid(1);

        UUID id1 = ig1.cluster().id();

        assertEquals(id0, id1);

        stopAllGrids();

        ig0 = startGrid(0);

        assertNotSame(id0, ig0.cluster().id());

        IgniteEx cl0 = startGrid("client0");

        assertEquals(ig0.cluster().id(), cl0.cluster().id());
    }

    /**
     * Test verifies that reconnected client applies newly generated ID and tag
     * and throws away values from old cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryClusterIdWithClientReconnect() throws Exception {
        IgniteClusterEx cluster0 = startGrid(0).cluster();

        UUID oldId = cluster0.id();
        String oldTag = cluster0.tag();

        IgniteEx client0 = startGrid("client0");

        AtomicBoolean reconnectEvent = new AtomicBoolean(false);

        client0.events().localListen((e) -> {
            reconnectEvent.set(true);

            return true;
        }, EventType.EVT_CLIENT_NODE_RECONNECTED);

        assertEquals(oldId, client0.cluster().id());
        assertEquals(oldTag, client0.cluster().tag());

        stopGrid(0);

        cluster0 = startGrid(0).cluster();

        assertNotSame(oldId, cluster0.id());
        assertNotSame(oldTag, cluster0.tag());

        assertTrue(GridTestUtils.waitForCondition(reconnectEvent::get, 10_000));

        assertEquals("OldID " + oldId, cluster0.id(), client0.cluster().id());
        assertEquals(cluster0.tag(), client0.cluster().tag());
    }

    /**
     * Verifies that in persistent-enabled cluster ID is not lost upon cluster restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistentClusterId() throws Exception {
        isPersistenceEnabled = true;

        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        assertTrue(GridTestUtils.waitForCondition(() -> ig0.cluster().id() != null, 10_000));

        final UUID id0 = ig0.cluster().id();

        checkStoredWithPers(metastorage(0), ig0, "key", "value");

        stopAllGrids();

        final IgniteEx ig01 = startGrid(0);

        assertTrue(GridTestUtils.waitForCondition(() -> id0.equals(ig01.cluster().id()), 10_000));
    }

    /**
     * Test verifies consistency of tag changes in cluster:
     * <ul>
     *     <li>Consistency across all server nodes when changed from a specific server node.</li>
     *     <li>Consistency across joining nodes including clients.</li>
     *     <li>Consistency across clients and servers when changed from client.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryClusterTag() throws Exception {
        IgniteEx ig0 = startGrid(0);

        String tag0 = ig0.cluster().tag();

        assertNotNull(tag0);

        ig0.cluster().tag(CUSTOM_TAG_0);

        IgniteEx ig1 = startGrid(1);

        String tag1 = ig1.cluster().tag();

        assertNotNull(tag1);

        assertEquals(CUSTOM_TAG_0, tag1);

        IgniteEx ig2 = startGrid(2);

        assertEquals(CUSTOM_TAG_0, ig2.cluster().tag());

        ig2.cluster().tag(CUSTOM_TAG_1);

        //tag set from one server node is applied on all other nodes
        assertEquals(CUSTOM_TAG_1, ig0.cluster().tag());

        assertEquals(CUSTOM_TAG_1, ig1.cluster().tag());

        IgniteEx cl0 = startGrid("client0");

        assertEquals(CUSTOM_TAG_1, cl0.cluster().tag());

        cl0.cluster().tag(CLIENT_CUSTOM_TAG_0);

        //tag set from client is applied on server nodes
        assertEquals(CLIENT_CUSTOM_TAG_0, ig0.cluster().tag());

        IgniteEx cl1 = startGrid("client1");

        cl1.cluster().tag(CLIENT_CUSTOM_TAG_1);

        //tag set from client is applied on other client nodes
        assertTrue(GridTestUtils.waitForCondition(() -> CLIENT_CUSTOM_TAG_1.equals(cl0.cluster().tag()), 5_000));
    }

    /**
     * Verifies restrictions for new tag provided for {@link IgniteCluster#tag(String)} method:
     * <ol>
     *     <li>Not null.</li>
     *     <li>Non-empty.</li>
     *     <li>Below 280 symbols (max tag length).</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTagExceptions() throws Exception {
        IgniteEx ig0 = startGrid(0);

        try {
            ig0.cluster().tag(null);

            fail("Expected exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().contains("cannot be null"));
        }

        try {
            ig0.cluster().tag("");

            fail("Expected exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().contains("should not be empty"));
        }

        String longString = new String(new char[281]);

        try {
            ig0.cluster().tag(longString);

            fail("Expected exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().contains("Maximum tag length is exceeded"));
        }
    }

    /**
     *  Verifies consistency of tag when set up in inactive and active clusters and on client nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistentClusterTag() throws Exception {
        isPersistenceEnabled = true;

        final IgniteEx ig0 = startGrid(0);

        try {
            ig0.cluster().tag(CUSTOM_TAG_0);

            fail("Expected exception has not been thrown.");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().contains("Can not change cluster tag on inactive cluster."));
        }

        final IgniteEx ig1 = startGrid(1);

        ig0.cluster().state(ClusterState.ACTIVE);

        assertTrue(GridTestUtils.waitForCondition(() -> ig0.cluster().tag() != null, 10_000));

        assertTrue(GridTestUtils.waitForCondition(() -> (ig0.cluster().tag().equals(ig1.cluster().tag())), 10_000));

        String tag1 = ig1.cluster().tag();

        checkStoredWithPers(metastorage(0), ig0, "key", "value");

        stopAllGrids();

        final IgniteEx ig01 = startGrid(0);

        IgniteEx ig11 = startGrid(1);

        assertTrue(GridTestUtils.waitForCondition(() -> tag1.equals(ig01.cluster().tag()), 10_000));

        ig11.cluster().state(ClusterState.ACTIVE);

        IgniteEx cl0 = startGrid("client0");

        cl0.cluster().tag(CUSTOM_TAG_0);

        stopAllGrids();

        startGrid(0);

        final IgniteEx ig12 = startGrid(1);

        assertTrue(GridTestUtils.waitForCondition(() -> CUSTOM_TAG_0.equals(ig12.cluster().tag()), 10_000));
    }

    /**
     * Verifies that event is fired when tag change request sent by user is completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTagChangedEvent() throws Exception {
        IgniteEx ig = startGrid(0);

        UUID clusterId = ig.cluster().id();
        String generatedTag = ig.cluster().tag();

        AtomicReference<UUID> clusterIdFromEvent = new AtomicReference<>(null);
        AtomicReference<String> oldTagFromEvent = new AtomicReference<>(null);
        AtomicReference<String> newTagFromEvent = new AtomicReference<>(null);

        AtomicBoolean evtFired = new AtomicBoolean(false);

        ig.cluster().state(ClusterState.ACTIVE);

        ig.events().localListen((evt) ->
            {
                evtFired.set(true);

                ClusterTagUpdatedEvent tagUpdatedEvt = (ClusterTagUpdatedEvent)evt;

                clusterIdFromEvent.set(tagUpdatedEvt.clusterId());
                oldTagFromEvent.set(tagUpdatedEvt.previousTag());
                newTagFromEvent.set(tagUpdatedEvt.newTag());

                return true;
            },
            EventType.EVT_CLUSTER_TAG_UPDATED);

        ig.cluster().tag(CUSTOM_TAG_0);

        assertTrue(GridTestUtils.waitForCondition(evtFired::get, 10_000));

        assertEquals(clusterId, clusterIdFromEvent.get());
        assertEquals(generatedTag, oldTagFromEvent.get());
        assertEquals(CUSTOM_TAG_0, newTagFromEvent.get());
    }

    /**
     * Verifies that event about cluster tag update is fired on remote nodes as well.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTagChangedEventMultinodeWithRemoteFilter() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteEx ig1 = startGrid(1);

        UUID clusterId = ig0.cluster().id();
        String generatedTag = ig0.cluster().tag();

        AtomicReference<UUID> eventNodeId = new AtomicReference<>(null);

        AtomicReference<UUID> clusterIdFromEvent = new AtomicReference<>(null);
        AtomicReference<String> oldTagFromEvent = new AtomicReference<>(null);
        AtomicReference<String> newTagFromEvent = new AtomicReference<>(null);

        AtomicBoolean evtFired = new AtomicBoolean(false);

        ig0.events(ig0.cluster().forRemotes()).remoteListen(
            (IgniteBiPredicate<UUID, Event>)(uuid, event) -> {
                eventNodeId.set(uuid);

                evtFired.set(true);

                ClusterTagUpdatedEvent tagUpdatedEvt = (ClusterTagUpdatedEvent)event;

                clusterIdFromEvent.set(tagUpdatedEvt.clusterId());
                oldTagFromEvent.set(tagUpdatedEvt.previousTag());
                newTagFromEvent.set(tagUpdatedEvt.newTag());

                return true;
            },
            (IgnitePredicate<Event>)event -> event.type() == EventType.EVT_CLUSTER_TAG_UPDATED);

        ig0.cluster().tag(CUSTOM_TAG_0);

        assertTrue(GridTestUtils.waitForCondition(evtFired::get, 10_000));

        assertEquals(ig1.localNode().id(), eventNodeId.get());

        assertEquals(clusterId, clusterIdFromEvent.get());
        assertEquals(generatedTag, oldTagFromEvent.get());
        assertEquals(CUSTOM_TAG_0, newTagFromEvent.get());
    }

    /**
     * @return {@link DistributedMetaStorage} instance for i'th node.
     */
    protected DistributedMetaStorage metastorage(int i) {
        return grid(i).context().distributedMetastorage();
    }

    /** Checks that appropriate key, value are stored into local metastore. */
    protected void checkStoredWithPers(
        DistributedMetaStorage msToStore,
        IgniteEx instanceToCheck,
        String key,
        String value
    ) throws IgniteCheckedException {
        assertTrue(isPersistenceEnabled);

        final CountDownLatch latch = new CountDownLatch(1);

        final DistributedMetaStorageImpl distrMetaStore =
            (DistributedMetaStorageImpl)instanceToCheck.context().distributedMetastorage();

        DmsDataWriterWorker worker = GridTestUtils.getFieldValue(distrMetaStore, "worker");

        ReadWriteMetastorage metastorage = GridTestUtils.getFieldValue(worker, "metastorage");

        assertNotNull(metastorage);

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            try {
                latch.await();

                assertTrue(waitForCondition(() -> {
                    try {
                        AtomicReference<String> contains = new AtomicReference<>();

                        metastorage.iterate("", (k, v) -> {
                            if (k.contains(key))
                                contains.set(k);
                        }, false);

                        return contains.get() != null && metastorage.readRaw(contains.get()) != null;
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }, 15_000));
            }
            catch (IgniteInterruptedCheckedException | InterruptedException e) {
                throw new IgniteException(e);
            }
        });

        latch.countDown();

        msToStore.write(key, value);

        f.get(getTestTimeout());
    }
}
