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

package org.apache.ignite.internal.metastorage.client;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.metastorage.server.EntryEvent;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.RaftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Meta storage client tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
public class ITMetaStorageServiceTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITMetaStorageServiceTest.class);

    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private static final int NODES = 2;

    /** */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "METASTORAGE_RAFT_GROUP";

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /**  Expected server result entry. */
    private static final org.apache.ignite.internal.metastorage.server.Entry EXPECTED_SRV_RESULT_ENTRY =
        new org.apache.ignite.internal.metastorage.server.Entry(
            new byte[] {1},
            new byte[] {2},
            10,
            2
        );

    /**
     * Expected server result entry.
     */
    private static final EntryImpl EXPECTED_RESULT_ENTRY =
        new EntryImpl(
            new ByteArray(new byte[] {1}),
            new byte[] {2},
            10,
            2
        );

    /**
     * Expected result map.
     */
    private static final NavigableMap<ByteArray, Entry> EXPECTED_RESULT_MAP;

    /** Expected server result collection. */
    private static final Collection<org.apache.ignite.internal.metastorage.server.Entry> EXPECTED_SRV_RESULT_COLL;

    /** Node 0 id. */
    private static final String NODE_ID_0 = "node-id-0";

    /** Node 1 id. */
    private static final String NODE_ID_1 = "node-id-1";

    /** Cluster. */
    private final ArrayList<ClusterService> cluster = new ArrayList<>();

    /** Meta storage raft server. */
    private RaftServer metaStorageRaftSrv;

    /** Raft group service. */
    private RaftGroupService metaStorageRaftGrpSvc;

    /** Mock Metastorage storage. */
    @Mock
    private KeyValueStorage mockStorage;

    /** Metastorage service. */
    private MetaStorageService metaStorageSvc;

    /** */
    @WorkDirectory
    private Path dataPath;

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    static {
        EXPECTED_RESULT_MAP = new TreeMap<>();

        EntryImpl entry1 = new EntryImpl(
            new ByteArray(new byte[] {1}),
            new byte[] {2},
            10,
            2
        );

        EXPECTED_RESULT_MAP.put(entry1.key(), entry1);

        EntryImpl entry2 = new EntryImpl(
            new ByteArray(new byte[] {3}),
            new byte[] {4},
            10,
            3
        );

        EXPECTED_RESULT_MAP.put(entry2.key(), entry2);

        EXPECTED_SRV_RESULT_COLL = List.of(
            new org.apache.ignite.internal.metastorage.server.Entry(
                entry1.key().bytes(), entry1.value(), entry1.revision(), entry1.updateCounter()
            ),
            new org.apache.ignite.internal.metastorage.server.Entry(
                entry2.key().bytes(), entry2.value(), entry2.revision(), entry2.updateCounter()
            )
        );
    }

    /**
     * Run {@code NODES} cluster nodes.
     */
    @BeforeEach
    public void beforeTest(TestInfo testInfo) throws Exception {
        List<NetworkAddress> localAddresses = findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + NODES);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        localAddresses.stream()
            .map(
                addr -> ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    nodeFinder,
                    SERIALIZATION_REGISTRY,
                    NETWORK_FACTORY
                )
            )
            .forEach(clusterService -> {
                clusterService.start();
                cluster.add(clusterService);
            });

        for (ClusterService node : cluster)
            assertTrue(waitForTopology(node, NODES, 1000));

        LOG.info("Cluster started.");

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME));

        metaStorageSvc = prepareMetaStorage();
    }

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        metaStorageRaftSrv.stop();
        metaStorageRaftGrpSvc.shutdown();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        for (ClusterService node : cluster)
            node.stop();
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key().bytes())).thenReturn(EXPECTED_SRV_RESULT_ENTRY);

        assertEquals(EXPECTED_RESULT_ENTRY, metaStorageSvc.get(EXPECTED_RESULT_ENTRY.key()).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetWithUpperBoundRevision() throws Exception {
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key().bytes(), EXPECTED_RESULT_ENTRY.revision()))
            .thenReturn(EXPECTED_SRV_RESULT_ENTRY);

        assertEquals(
            EXPECTED_RESULT_ENTRY,
            metaStorageSvc.get(EXPECTED_RESULT_ENTRY.key(), EXPECTED_RESULT_ENTRY.revision()).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#getAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAll() throws Exception {
        when(mockStorage.getAll(anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        assertEquals(EXPECTED_RESULT_MAP, metaStorageSvc.getAll(EXPECTED_RESULT_MAP.keySet()).get());
    }

    /**
     * Tests {@link MetaStorageService#getAll(Set, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllWithUpperBoundRevision() throws Exception {
        when(mockStorage.getAll(anyList(), eq(10L))).thenReturn(EXPECTED_SRV_RESULT_COLL);

        assertEquals(
            EXPECTED_RESULT_MAP,
            metaStorageSvc.getAll(EXPECTED_RESULT_MAP.keySet(), 10).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#put(ByteArray, byte[])}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        ByteArray expKey = new ByteArray(new byte[] {1});

        byte[] expVal = {2};

        doNothing().when(mockStorage).put(expKey.bytes(), expVal);

        metaStorageSvc.put(expKey, expVal).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndPut(ByteArray, byte[])}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPut() throws Exception {
        byte[] expVal = {2};

        when(mockStorage.getAndPut(EXPECTED_RESULT_ENTRY.key().bytes(), expVal)).thenReturn(EXPECTED_SRV_RESULT_ENTRY);

        assertEquals(
            EXPECTED_RESULT_ENTRY,
            metaStorageSvc.getAndPut(EXPECTED_RESULT_ENTRY.key(), expVal).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#putAll(Map)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutAll() throws Exception {
        metaStorageSvc.putAll(
            EXPECTED_RESULT_MAP.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().value())
                )
        ).get();

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<byte[]>> valuesCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).putAll(keysCaptor.capture(), valuesCaptor.capture());

        // Assert keys equality.
        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
            map(ByteArray::bytes).collect(toList());

        for (int i = 0; i < expKeys.size(); i++)
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));

        // Assert values equality.
        assertEquals(EXPECTED_RESULT_MAP.values().size(), valuesCaptor.getValue().size());

        List<byte[]> expVals = EXPECTED_RESULT_MAP.values().stream().
            map(Entry::value).collect(toList());

        for (int i = 0; i < expKeys.size(); i++)
            assertArrayEquals(expVals.get(i), valuesCaptor.getValue().get(i));
    }

    /**
     * Tests {@link MetaStorageService#getAndPutAll(Map)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPutAll() throws Exception {
        when(mockStorage.getAndPutAll(anyList(), anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        Map<ByteArray, Entry> gotRes = metaStorageSvc.getAndPutAll(
            EXPECTED_RESULT_MAP.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().value())
                )
        ).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<byte[]>> valuesCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).getAndPutAll(keysCaptor.capture(), valuesCaptor.capture());

        // Assert keys equality.
        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
            map(ByteArray::bytes).collect(toList());

        for (int i = 0; i < expKeys.size(); i++)
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));

        // Assert values equality.
        assertEquals(EXPECTED_RESULT_MAP.values().size(), valuesCaptor.getValue().size());

        List<byte[]> expVals = EXPECTED_RESULT_MAP.values().stream().
            map(Entry::value).collect(toList());

        for (int i = 0; i < expKeys.size(); i++)
            assertArrayEquals(expVals.get(i), valuesCaptor.getValue().get(i));
    }

    /**
     * Tests {@link MetaStorageService#remove(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        ByteArray expKey = new ByteArray(new byte[] {1});

        doNothing().when(mockStorage).remove(expKey.bytes());

        metaStorageSvc.remove(expKey).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndRemove(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemove() throws Exception {
        EntryImpl expRes = new EntryImpl(
            new ByteArray(new byte[] {1}),
            new byte[] {3},
            10,
            2
        );

        when(mockStorage.getAndRemove(expRes.key().bytes())).thenReturn(
            new org.apache.ignite.internal.metastorage.server.Entry(
                expRes.key().bytes(),
                expRes.value(),
                expRes.revision(),
                expRes.updateCounter()
            )
        );

        assertEquals(expRes, metaStorageSvc.getAndRemove(expRes.key()).get());
    }

    /**
     * Tests {@link MetaStorageService#removeAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        doNothing().when(mockStorage).removeAll(anyList());

        metaStorageSvc.removeAll(EXPECTED_RESULT_MAP.keySet()).get();

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
            map(ByteArray::bytes).collect(toList());

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).removeAll(keysCaptor.capture());

        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        for (int i = 0; i < expKeys.size(); i++)
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));
    }

    /**
     * Tests {@link MetaStorageService#getAndRemoveAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemoveAll() throws Exception {
        when(mockStorage.getAndRemoveAll(anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        Map<ByteArray, Entry> gotRes = metaStorageSvc.getAndRemoveAll(EXPECTED_RESULT_MAP.keySet()).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).getAndRemoveAll(keysCaptor.capture());

        // Assert keys equality.
        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
            map(ByteArray::bytes).collect(toList());

        for (int i = 0; i < expKeys.size(); i++)
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with not null keyTo and explicit
     * revUpperBound.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeWitKeyToAndUpperBound() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[] {1});

        ByteArray expKeyTo = new ByteArray(new byte[] {3});

        long expRevUpperBound = 10;

        when(mockStorage.range(expKeyFrom.bytes(), expKeyTo.bytes(), expRevUpperBound)).thenReturn(mock(Cursor.class));

        metaStorageSvc.range(expKeyFrom, expKeyTo, expRevUpperBound).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with not null keyTo.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeWitKeyTo() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[] {1});

        ByteArray expKeyTo = new ByteArray(new byte[] {3});

        when(mockStorage.range(expKeyFrom.bytes(), expKeyTo.bytes())).thenReturn(mock(Cursor.class));

        metaStorageSvc.range(expKeyFrom, expKeyTo).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with null keyTo.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeWitNullAsKeyTo() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[] {1});

        when(mockStorage.range(expKeyFrom.bytes(), null)).thenReturn(mock(Cursor.class));

        metaStorageSvc.range(expKeyFrom, null).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} hasNext.
     */
    @Test
    public void testRangeHasNext() {
        ByteArray expKeyFrom = new ByteArray(new byte[] {1});

        when(mockStorage.range(expKeyFrom.bytes(), null)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);

            return cursor;
        });

        Cursor<Entry> cursor = metaStorageSvc.range(expKeyFrom, null);

        assertTrue(cursor.iterator().hasNext());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} next.
     *
     */
    @Test
    public void testRangeNext() {
        when(mockStorage.range(EXPECTED_RESULT_ENTRY.key().bytes(), null)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(EXPECTED_SRV_RESULT_ENTRY);

            return cursor;
        });

        Cursor<Entry> cursor = metaStorageSvc.range(EXPECTED_RESULT_ENTRY.key(), null);

        assertEquals(EXPECTED_RESULT_ENTRY, cursor.iterator().next());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} close.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeClose() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[] {1});

        Cursor cursorMock = mock(Cursor.class);

        when(mockStorage.range(expKeyFrom.bytes(), null)).thenReturn(cursorMock);

        Cursor<Entry> cursor = metaStorageSvc.range(expKeyFrom, null);

        cursor.close();

        verify(cursorMock, times(1)).close();
    }

    @Test
    public void testWatchOnUpdate() throws Exception {
        org.apache.ignite.internal.metastorage.server.WatchEvent expectedEvent =
            new org.apache.ignite.internal.metastorage.server.WatchEvent(List.of(
                new org.apache.ignite.internal.metastorage.server.EntryEvent(
                    new org.apache.ignite.internal.metastorage.server.Entry(
                        new byte[] {2},
                        new byte[] {20},
                        1,
                        1
                    ),
                    new org.apache.ignite.internal.metastorage.server.Entry(
                        new byte[] {2},
                        new byte[] {21},
                        2,
                        4
                    )
                ),
                new org.apache.ignite.internal.metastorage.server.EntryEvent(
                    new org.apache.ignite.internal.metastorage.server.Entry(
                        new byte[] {3},
                        new byte[] {20},
                        1,
                        2
                    ),
                    new org.apache.ignite.internal.metastorage.server.Entry(
                        new byte[] {3},
                        new byte[] {},
                        2,
                        5
                    )
                ),
                new org.apache.ignite.internal.metastorage.server.EntryEvent(
                    new org.apache.ignite.internal.metastorage.server.Entry(
                        new byte[] {4},
                        new byte[] {20},
                        1,
                        3
                    ),
                    new org.apache.ignite.internal.metastorage.server.Entry(
                        new byte[] {4},
                        new byte[] {},
                        3,
                        6
                    )
                )
            ));

        ByteArray keyFrom = new ByteArray(new byte[] {1});

        ByteArray keyTo = new ByteArray(new byte[] {10});

        long rev = 2;

        when(mockStorage.watch(keyFrom.bytes(), keyTo.bytes(), rev)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(expectedEvent);

            return cursor;
        });

        CountDownLatch latch = new CountDownLatch(1);

        IgniteUuid watchId = metaStorageSvc.watch(keyFrom, keyTo, rev, new WatchListener() {
            @Override public boolean onUpdate(@NotNull WatchEvent event) {
                Collection<EntryEvent> expectedEvents = expectedEvent.entryEvents();
                Collection<org.apache.ignite.internal.metastorage.client.EntryEvent> actualEvents = event.entryEvents();

                assertEquals(expectedEvents.size(), actualEvents.size());

                Iterator<EntryEvent> expectedIterator = expectedEvents.iterator();
                Iterator<org.apache.ignite.internal.metastorage.client.EntryEvent> actualIterator = actualEvents.iterator();

                while (expectedIterator.hasNext() && actualIterator.hasNext()) {
                    org.apache.ignite.internal.metastorage.server.EntryEvent expectedEntryEvent = expectedIterator.next();
                    org.apache.ignite.internal.metastorage.client.EntryEvent actualEntryEvent = actualIterator.next();

                    assertArrayEquals(expectedEntryEvent.oldEntry().key(), actualEntryEvent.oldEntry().key().bytes());
                    assertArrayEquals(expectedEntryEvent.oldEntry().value(), actualEntryEvent.oldEntry().value());
                    assertArrayEquals(expectedEntryEvent.entry().key(), actualEntryEvent.newEntry().key().bytes());
                    assertArrayEquals(expectedEntryEvent.entry().value(), actualEntryEvent.newEntry().value());
                }

                latch.countDown();

                return true;
            }

            @Override public void onError(@NotNull Throwable e) {
                // Within given test it's not expected to get here.
                fail();
            }
        }).get();

        latch.await();

        metaStorageSvc.stopWatch(watchId).get();
    }

    @Test
    public void testInvoke() throws Exception {
        ByteArray expKey = new ByteArray(new byte[] {1});

        byte[] expVal = {2};

        when(mockStorage.invoke(any(), any(), any())).thenReturn(true);

        Condition condition = Conditions.notExists(expKey);

        Operation success = Operations.put(expKey, expVal);

        Operation failure = Operations.noop();

        assertTrue(metaStorageSvc.invoke(condition, success, failure).get());

        var conditionCaptor = ArgumentCaptor.forClass(org.apache.ignite.internal.metastorage.server.Condition.class);

        ArgumentCaptor<Collection<org.apache.ignite.internal.metastorage.server.Operation>> successCaptor =
            ArgumentCaptor.forClass(Collection.class);

        ArgumentCaptor<Collection<org.apache.ignite.internal.metastorage.server.Operation>> failureCaptor =
            ArgumentCaptor.forClass(Collection.class);

        verify(mockStorage).invoke(conditionCaptor.capture(), successCaptor.capture(), failureCaptor.capture());

        assertArrayEquals(expKey.bytes(), conditionCaptor.getValue().key());

        assertArrayEquals(expKey.bytes(), successCaptor.getValue().iterator().next().key());
        assertArrayEquals(expVal, successCaptor.getValue().iterator().next().value());

        assertEquals(OperationType.NO_OP, failureCaptor.getValue().iterator().next().type());
    }

    // TODO: IGNITE-14693 Add tests for exception handling logic: onError,
    // TODO: (CompactedException | OperationTimeoutException)

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     */
    @Disabled // TODO: IGNITE-14693 Add tests for exception handling logic.
    @Test
    public void testGetThatThrowsCompactedException() {
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key().bytes()))
            .thenThrow(new org.apache.ignite.internal.metastorage.server.CompactedException());

        assertThrows(CompactedException.class, () -> metaStorageSvc.get(EXPECTED_RESULT_ENTRY.key()).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     */
    @Disabled // TODO: IGNITE-14693 Add tests for exception handling logic.
    @Test
    public void testGetThatThrowsOperationTimeoutException() {
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key().bytes())).thenThrow(new OperationTimeoutException());

        assertThrows(OperationTimeoutException.class, () -> metaStorageSvc.get(EXPECTED_RESULT_ENTRY.key()).get());
    }

    /**
     * Tests {@link MetaStorageService#closeCursors(String)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCursorsCleanup() throws Exception {
        when(mockStorage.range(EXPECTED_RESULT_ENTRY.key().bytes(), null)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(EXPECTED_SRV_RESULT_ENTRY);

            return cursor;
        });

        List<Peer> peers = List.of(new Peer(cluster.get(0).topologyService().localMember().address()));

        RaftGroupService metaStorageRaftGrpSvc = RaftGroupServiceImpl.start(
            METASTORAGE_RAFT_GROUP_NAME,
            cluster.get(1),
            FACTORY,
            10_000,
            peers,
            true,
            200,
            executor
        ).get(3, TimeUnit.SECONDS);

        try {
            MetaStorageService metaStorageSvc2 = new MetaStorageServiceImpl(metaStorageRaftGrpSvc, NODE_ID_1);

            Cursor<Entry> cursorNode0 = metaStorageSvc.range(EXPECTED_RESULT_ENTRY.key(), null);

            Cursor<Entry> cursor2Node0 = metaStorageSvc.range(EXPECTED_RESULT_ENTRY.key(), null);

            Cursor<Entry> cursorNode1 = metaStorageSvc2.range(EXPECTED_RESULT_ENTRY.key(), null);

            metaStorageSvc.closeCursors(NODE_ID_0).get();

            assertThrows(NoSuchElementException.class, () -> cursorNode0.iterator().next());

            assertThrows(NoSuchElementException.class, () -> cursor2Node0.iterator().next());

            assertEquals(EXPECTED_RESULT_ENTRY, (cursorNode1.iterator().next()));
        }
        finally {
            metaStorageRaftGrpSvc.shutdown();
        }
    }

    /**
     * @param cluster The cluster.
     * @param exp     Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    @SuppressWarnings("SameParameterValue")
    private static boolean waitForTopology(ClusterService cluster, int exp, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cluster.topologyService().allMembers().size() >= exp)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * Prepares meta storage by instantiating corresponding raft server with {@link MetaStorageListener} and {@link
     * MetaStorageServiceImpl}.
     *
     * @return {@link MetaStorageService} instance.
     */
    private MetaStorageService prepareMetaStorage() throws Exception {
        List<Peer> peers = List.of(new Peer(cluster.get(0).topologyService().localMember().address()));

        metaStorageRaftSrv = new RaftServerImpl(cluster.get(0), FACTORY);

        metaStorageRaftSrv.start();

        metaStorageRaftSrv.startRaftGroup(METASTORAGE_RAFT_GROUP_NAME, new MetaStorageListener(mockStorage), peers);

        metaStorageRaftGrpSvc = RaftGroupServiceImpl.start(
            METASTORAGE_RAFT_GROUP_NAME,
            cluster.get(1),
            FACTORY,
            10_000,
            peers,
            true,
            200,
            executor
        ).get(3, TimeUnit.SECONDS);

        return new MetaStorageServiceImpl(metaStorageRaftGrpSvc, NODE_ID_0);
    }
}
