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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageCommandListener;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartMessage;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartMessageSerializationFactory;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartResponseMessageSerializationFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.scalecube.message.ScaleCubeMessage;
import org.apache.ignite.network.scalecube.message.ScaleCubeMessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.RaftServer;
import org.apache.ignite.raft.server.impl.RaftServerImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Meta storage client tests.
 */
@SuppressWarnings("WeakerAccess")
public class ITMetaStorageServiceTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITMetaStorageServiceTest.class);

    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private static final int NODES = 2;

    /** */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "METASTORAGE_RAFT_GROUP";

    /** */
    public static final int LATEST_REVISION = -1;

    /** Factory. */
    private static final RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry()
        .registerFactory(ScaleCubeMessage.TYPE, new ScaleCubeMessageSerializationFactory())
        .registerFactory(HandshakeStartMessage.TYPE, new HandshakeStartMessageSerializationFactory())
        .registerFactory(HandshakeStartResponseMessage.TYPE, new HandshakeStartResponseMessageSerializationFactory());

    /**  Expected server result entry. */
    private static final org.apache.ignite.internal.metastorage.server.Entry EXPECTED_SRV_RESULT_ENTRY =
            new org.apache.ignite.internal.metastorage.server.Entry(
                    new byte[] {1},
                    new byte[] {2},
                    10,
                    2
            );

    /**  Expected server result entry. */
    private static final EntryImpl EXPECTED_RESULT_ENTRY =
            new EntryImpl(
                    new ByteArray(new byte[] {1}),
                    new byte[] {2},
                    10,
                    2
            );

    /** Expected result map. */
    private static final NavigableMap<ByteArray, Entry> EXPECTED_RESULT_MAP;

    private static final Collection<org.apache.ignite.internal.metastorage.server.Entry> EXPECTED_SRV_RESULT_COLL;

    /** Cluster. */
    private ArrayList<ClusterService> cluster = new ArrayList<>();

    /**  Meta storage raft server. */
    private RaftServer metaStorageRaftSrv;

    static {
        EXPECTED_RESULT_MAP = new TreeMap<>();

        EntryImpl entry1 = new EntryImpl(
                new ByteArray(new byte[]{1}),
                new byte[]{2},
                10,
                2
        );

        EXPECTED_RESULT_MAP.put(entry1.key(), entry1);

        EntryImpl entry2 = new EntryImpl(
                new ByteArray(new byte[]{3}),
                new byte[]{4},
                10,
                3
        );

        EXPECTED_RESULT_MAP.put(entry2.key(), entry2);

        EXPECTED_SRV_RESULT_COLL = new ArrayList<>();

        EXPECTED_SRV_RESULT_COLL.add(new org.apache.ignite.internal.metastorage.server.Entry(
                entry1.key().bytes(), entry1.value(), entry1.revision(), entry1.updateCounter()
        ));

        EXPECTED_SRV_RESULT_COLL.add(new org.apache.ignite.internal.metastorage.server.Entry(
                entry2.key().bytes(), entry2.value(), entry2.revision(), entry2.updateCounter()
        ));
    }

    /**
     * Run @{code} NODES cluster nodes.
     */
    @BeforeEach
    public void beforeTest() {
        for (int i = 0; i < NODES; i++) {
            cluster.add(
                    startClusterNode(
                            "node_" + i,
                            NODE_PORT_BASE + i,
                            IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES).boxed().
                                    map((port) -> "localhost:" + port).collect(Collectors.toList())));
        }

        for (ClusterService node : cluster)
            assertTrue(waitForTopology(node, NODES, 1000));

        LOG.info("Cluster started.");
    }

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        metaStorageRaftSrv.shutdown();

        for (ClusterService node : cluster)
            node.shutdown();
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key) {
                        return EXPECTED_SRV_RESULT_ENTRY;
                    }
                });

        assertEquals(EXPECTED_RESULT_ENTRY, metaStorageSvc.get(EXPECTED_RESULT_ENTRY.key()).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetWithUpperBoundRevision() throws Exception {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key, long rev) {
                        return EXPECTED_SRV_RESULT_ENTRY;
                    }
                });

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
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull List<org.apache.ignite.internal.metastorage.server.Entry> getAll(List<byte[]> keys) {
                        return new ArrayList<>(EXPECTED_SRV_RESULT_COLL);
                    }
                });

        assertEquals(EXPECTED_RESULT_MAP, metaStorageSvc.getAll(EXPECTED_RESULT_MAP.keySet()).get());
    }

    /**
     * Tests {@link MetaStorageService#getAll(Set, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllWithUpperBoundRevision() throws Exception {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull List<org.apache.ignite.internal.metastorage.server.Entry> getAll(List<byte[]> keys, long revUpperBound) {
                        return new ArrayList<>(EXPECTED_SRV_RESULT_COLL);
                    }
                });

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
        ByteArray expKey = new ByteArray(new byte[]{1});

        byte[] expVal = new byte[]{2};

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @SuppressWarnings("JavaAbbreviationUsage")
                    @Override public void put(byte[] key, byte[] value) {
                        assertArrayEquals(expKey.bytes(), key);

                        assertArrayEquals(expVal, value);
                    }
                });

        metaStorageSvc.put(expKey, expVal).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndPut(ByteArray, byte[])}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPut() throws Exception {
        byte[] expVal = new byte[]{2};

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @SuppressWarnings("JavaAbbreviationUsage")
                    @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry getAndPut(byte[] key, byte[] value) {
                        assertArrayEquals(EXPECTED_RESULT_ENTRY.key().bytes(), key);

                        assertArrayEquals(expVal, value);

                        return EXPECTED_SRV_RESULT_ENTRY;
                    }
                });

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
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
                        // Assert keys equality.
                        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keys.size());

                        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
                                map(ByteArray::bytes).collect(Collectors.toList());

                        for (int i = 0; i < expKeys.size(); i++)
                            assertArrayEquals(expKeys.get(i), keys.get(i));


                        // Assert values equality.
                        assertEquals(EXPECTED_RESULT_MAP.values().size(), values.size());

                        List<byte[]> expVals = EXPECTED_RESULT_MAP.values().stream().
                                map(Entry::value).collect(Collectors.toList());

                        for (int i = 0; i < expKeys.size(); i++)
                            assertArrayEquals(expVals.get(i), values.get(i));
                    }
                });

        metaStorageSvc.putAll(
                EXPECTED_RESULT_MAP.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().value())
                        )
        ).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndPutAll(Map)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPutAll() throws Exception {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull List<org.apache.ignite.internal.metastorage.server.Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
                        // Assert keys equality.
                        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keys.size());

                        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
                                map(ByteArray::bytes).collect(Collectors.toList());

                        for (int i = 0; i < expKeys.size(); i++)
                            assertArrayEquals(expKeys.get(i), keys.get(i));

                        // Assert values equality.
                        assertEquals(EXPECTED_RESULT_MAP.values().size(), values.size());

                        List<byte[]> expVals = EXPECTED_RESULT_MAP.values().stream().
                                map(Entry::value).collect(Collectors.toList());

                        for (int i = 0; i < expKeys.size(); i++)
                            assertArrayEquals(expVals.get(i), values.get(i));

                        return new ArrayList<>(EXPECTED_SRV_RESULT_COLL);
                    }
                });

        Map<ByteArray, Entry> gotRes = metaStorageSvc.getAndPutAll(
                EXPECTED_RESULT_MAP.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().value())
                        )
        ).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);
    }

    /**
     * Tests {@link MetaStorageService#remove(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        ByteArray expKey = new ByteArray(new byte[]{1});

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public void remove(byte[] key) {
                        assertArrayEquals(expKey.bytes(), key);
                    }
                });

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
                new ByteArray(new byte[]{1}),
                new byte[]{3},
                10,
                2
        );

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry getAndRemove(byte[] key) {
                        assertArrayEquals(expRes.key().bytes(), key);

                        return new org.apache.ignite.internal.metastorage.server.Entry(
                                expRes.key().bytes(),
                                expRes.value(),
                                expRes.revision(),
                                expRes.updateCounter()
                        );
                    }
                });

        assertEquals(expRes, metaStorageSvc.getAndRemove(expRes.key()).get());
    }

    /**
     * Tests {@link MetaStorageService#removeAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public void removeAll(List<byte[]> keys) {
                        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keys.size());

                        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
                                map(ByteArray::bytes).collect(Collectors.toList());

                        for (int i = 0; i < expKeys.size(); i++)
                            assertArrayEquals(expKeys.get(i), keys.get(i));
                    }
                });

        metaStorageSvc.removeAll(EXPECTED_RESULT_MAP.keySet()).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndRemoveAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemoveAll() throws Exception {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull List<org.apache.ignite.internal.metastorage.server.Entry> getAndRemoveAll(List<byte[]> keys) {
                        // Assert keys equality.
                        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keys.size());

                        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream().
                                map(ByteArray::bytes).collect(Collectors.toList());

                        for (int i = 0; i < expKeys.size(); i++)
                            assertArrayEquals(expKeys.get(i), keys.get(i));

                        return new ArrayList<>(EXPECTED_SRV_RESULT_COLL);
                    }
                });

        Map<ByteArray, Entry> gotRes = metaStorageSvc.getAndRemoveAll(EXPECTED_RESULT_MAP.keySet()).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with not null keyTo and explicit revUpperBound.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeWitKeyToAndUpperBound() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        ByteArray expKeyTo = new ByteArray(new byte[]{3});

        long expRevUpperBound = 10;

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
                        assertArrayEquals(expKeyFrom.bytes(), keyFrom);

                        assertArrayEquals(expKeyTo.bytes(), keyTo);

                        assertEquals(expRevUpperBound, revUpperBound);

                        return new Cursor<>() {
                            private final Iterator<org.apache.ignite.internal.metastorage.server.Entry> it = new Iterator<>() {
                                @Override public boolean hasNext() {
                                    return false;
                                }

                                @Override public org.apache.ignite.internal.metastorage.server.Entry next() {
                                    return null;
                                }
                            };


                            @Override public void close() throws Exception {

                            }

                            @NotNull @Override public Iterator<org.apache.ignite.internal.metastorage.server.Entry> iterator() {
                                return it;
                            }

                            @Override
                            public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override
                            public org.apache.ignite.internal.metastorage.server.Entry next() {
                                return it.next();
                            }
                        };
                    }
                });

        metaStorageSvc.range(expKeyFrom, expKeyTo, expRevUpperBound).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with not null keyTo.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeWitKeyTo() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        ByteArray expKeyTo = new ByteArray(new byte[]{3});

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
                        assertArrayEquals(expKeyFrom.bytes(), keyFrom);

                        assertArrayEquals(expKeyTo.bytes(), keyTo);

                        assertEquals(LATEST_REVISION, revUpperBound);

                        return new Cursor<>() {
                            private final Iterator<org.apache.ignite.internal.metastorage.server.Entry> it = new Iterator<>() {
                                @Override
                                public boolean hasNext() {
                                    return false;
                                }

                                @Override
                                public org.apache.ignite.internal.metastorage.server.Entry next() {
                                    return null;
                                }
                            };

                            @Override public void close() throws Exception {

                            }

                            @NotNull @Override public Iterator<org.apache.ignite.internal.metastorage.server.Entry> iterator() {
                                return it;
                            }

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override public org.apache.ignite.internal.metastorage.server.Entry next() {
                                return it.next();
                            }
                        };
                    }
                });

        metaStorageSvc.range(expKeyFrom, expKeyTo).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with null keyTo.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeWitNullAsKeyTo() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
                        assertArrayEquals(expKeyFrom.bytes(), keyFrom);

                        assertNull(keyTo);

                        assertEquals(LATEST_REVISION, revUpperBound);

                        return new Cursor<>() {
                            private final Iterator<org.apache.ignite.internal.metastorage.server.Entry> it = new Iterator<>() {
                                @Override public boolean hasNext() {
                                    return false;
                                }

                                @Override public org.apache.ignite.internal.metastorage.server.Entry next() {
                                    return null;
                                }
                            };

                            @Override public void close() throws Exception {

                            }

                            @NotNull @Override public Iterator<org.apache.ignite.internal.metastorage.server.Entry> iterator() {
                                return it;
                            }

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override
                            public org.apache.ignite.internal.metastorage.server.Entry next() {
                                return it.next();
                            }
                        };
                    }
                });

        metaStorageSvc.range(expKeyFrom, null).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} hasNext.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeHasNext() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
                        return new Cursor<>() {
                            private final Iterator<org.apache.ignite.internal.metastorage.server.Entry> it = new Iterator<>() {
                                @Override public boolean hasNext() {
                                    return true;
                                }

                                @Override public org.apache.ignite.internal.metastorage.server.Entry next() {
                                    return null;
                                }
                            };

                            @Override public void close() throws Exception {

                            }

                            @NotNull @Override public Iterator<org.apache.ignite.internal.metastorage.server.Entry> iterator() {
                                return it;
                            }

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override
                            public org.apache.ignite.internal.metastorage.server.Entry next() {
                                return it.next();
                            }
                        };
                    }
                });

        Cursor<Entry> cursor = metaStorageSvc.range(expKeyFrom, null);

        assertTrue(cursor.iterator().hasNext());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} next.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeNext() throws Exception {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
                        return new Cursor<>() {
                            private final Iterator<org.apache.ignite.internal.metastorage.server.Entry> it = new Iterator<>() {
                                @Override public boolean hasNext() {
                                    return true;
                                }

                                @Override public org.apache.ignite.internal.metastorage.server.Entry next() {
                                    return EXPECTED_SRV_RESULT_ENTRY;
                                }
                            };

                            @Override public void close() throws Exception {

                            }

                            @NotNull @Override public Iterator<org.apache.ignite.internal.metastorage.server.Entry> iterator() {
                                return it;
                            }

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override
                            public org.apache.ignite.internal.metastorage.server.Entry next() {
                                return it.next();
                            }
                        };
                    }
                });

        Cursor<Entry> cursor = metaStorageSvc.range(EXPECTED_RESULT_ENTRY.key(), null);

        assertEquals(EXPECTED_RESULT_ENTRY, (cursor.iterator().next()));
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} close.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeClose() throws Exception {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        Cursor cursorMock = mock(Cursor.class);

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
                        return cursorMock;
                    }
                });

        Cursor<Entry> cursor = metaStorageSvc.range(expKeyFrom, null);

        cursor.close();

        verify(cursorMock, times(1)).close();
    }

    @Test
    public void testWatchOnUpdate() throws Exception {
        org.apache.ignite.internal.metastorage.server.WatchEvent returnedWatchEvents = new org.apache.ignite.internal.metastorage.server.WatchEvent(List.of(
                new org.apache.ignite.internal.metastorage.server.EntryEvent(
                        new org.apache.ignite.internal.metastorage.server.Entry(
                                new byte[]{2},
                                new byte[]{20},
                                1,
                                1
                        ),
                        new org.apache.ignite.internal.metastorage.server.Entry(
                                new byte[]{2},
                                new byte[]{21},
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
                                new byte[]{},
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

        ByteArray keyFrom = new ByteArray(new byte[]{1});

        ByteArray keyTo = new ByteArray(new byte[]{10});

        long rev = 2;

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public Cursor<org.apache.ignite.internal.metastorage.server.WatchEvent> watch(byte[] keyFrom, byte[] keyTo, long rev) {
                        return new Cursor<>() {
                            private final Iterator<org.apache.ignite.internal.metastorage.server.WatchEvent> it = new Iterator<>() {
                                @Override public boolean hasNext() {

                                    return retirevedItemCnt.get() < returnedWatchEvents.entryEvents().size();
                                }

                                @Override public org.apache.ignite.internal.metastorage.server.WatchEvent next() {
                                    return returnedWatchEvents;
                                }
                            };

                            AtomicInteger retirevedItemCnt = new AtomicInteger(0);

                            @Override public void close() throws Exception {
                                // No-op.
                            }

                            @NotNull @Override public Iterator<org.apache.ignite.internal.metastorage.server.WatchEvent> iterator() {
                                return it;
                            }

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override
                            public org.apache.ignite.internal.metastorage.server.WatchEvent next() {
                                return it.next();
                            }
                        };
                    }
                });

        CountDownLatch latch = new CountDownLatch(1);

        IgniteUuid watchId = metaStorageSvc.watch(keyFrom, keyTo, rev, new WatchListener() {
            @Override public boolean onUpdate(@NotNull WatchEvent events) {
                List gotEvents = new ArrayList();

                List returnedWatchEvents = new ArrayList(events.entryEvents());

                Iterator<EntryEvent> iter = events.entryEvents().iterator();

                while (iter.hasNext())
                    gotEvents.add(iter.next());

                assertEquals(3, gotEvents.size());

                assertTrue(gotEvents.contains(returnedWatchEvents.get(0)));

                assertTrue(gotEvents.contains(returnedWatchEvents.get(1)));

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
        ByteArray expKey = new ByteArray(new byte[]{1});

        byte[] expVal = new byte[]{2};

        Condition condition = Conditions.notExists(expKey);

        Operation success = Operations.put(expKey, expVal);

        Operation failure = Operations.noop();

        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public boolean invoke(
                            org.apache.ignite.internal.metastorage.server.Condition cond,
                            Collection<org.apache.ignite.internal.metastorage.server.Operation> success,
                            Collection<org.apache.ignite.internal.metastorage.server.Operation> failure) {
                        assertArrayEquals(expKey.bytes(), cond.key());

                        assertArrayEquals(expKey.bytes(), success.iterator().next().key());
                        assertArrayEquals(expVal, success.iterator().next().value());

                        assertEquals(OperationType.NO_OP, failure.iterator().next().type());

                        return true;
                    }
                });

        assertTrue(metaStorageSvc.invoke(condition, success, failure).get());
    }

    // TODO: IGNITE-14693 Add tests for exception handling logic: onError,
    // TODO: (CompactedException | OperationTimeoutException)

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Disabled // TODO: IGNITE-14693 Add tests for exception handling logic.
    @Test
    public void testGetThatThrowsCompactedException() {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key) {
                        throw new org.apache.ignite.internal.metastorage.server.CompactedException();
                    }
                });

        assertThrows(CompactedException.class, () -> metaStorageSvc.get(EXPECTED_RESULT_ENTRY.key()).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Disabled // TODO: IGNITE-14693 Add tests for exception handling logic.
    @Test
    public void testGetThatThrowsOperationTimeoutException() {
        MetaStorageService metaStorageSvc = prepareMetaStorage(
                new AbstractKeyValueStorage() {
                    @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key) {
                        throw new OperationTimeoutException();
                    }
                });

        assertThrows(OperationTimeoutException.class, () -> metaStorageSvc.get(EXPECTED_RESULT_ENTRY.key()).get());
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param srvs Server nodes of the cluster.
     * @return The client cluster view.
     */
    private ClusterService startClusterNode(String name, int port, List<String> srvs) {
        var ctx = new ClusterLocalConfiguration(name, port, srvs, SERIALIZATION_REGISTRY);

        var net = NETWORK_FACTORY.createClusterService(ctx);

        net.start();

        return net;
    }

    /**
     * @param cluster The cluster.
     * @param exp Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    @SuppressWarnings("SameParameterValue")
    private boolean waitForTopology(ClusterService cluster, int exp, int timeout) {
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
     * Prepares meta storage by instantiating corresponding raft server with MetaStorageCommandListener and
     * {@link MetaStorageServiceImpl}.
     *
     * @param keyValStorageMock {@link KeyValueStorage} mock.
     * @return {@link MetaStorageService} instance.
     */
    private MetaStorageService prepareMetaStorage(KeyValueStorage keyValStorageMock) {
        metaStorageRaftSrv = new RaftServerImpl(
                cluster.get(0),
                FACTORY,
                1000,
                Map.of(METASTORAGE_RAFT_GROUP_NAME, new MetaStorageCommandListener(keyValStorageMock))
        );

        RaftGroupService metaStorageRaftGrpSvc = new RaftGroupServiceImpl(
                METASTORAGE_RAFT_GROUP_NAME,
                cluster.get(1),
                FACTORY,
                10_000,
                List.of(new Peer(cluster.get(0).topologyService().localMember())),
                true,
                200
        );

        return new MetaStorageServiceImpl(metaStorageRaftGrpSvc);
    }

    /**
     * Abstract {@link KeyValueStorage}. Used for tests purposes.
     */
    @SuppressWarnings("JavaAbbreviationUsage")
    private abstract class AbstractKeyValueStorage implements KeyValueStorage {
        /** {@inheritDoc} */
        @Override public long revision() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long updateCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry get(byte[] key, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAll(List<byte[]> keys) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAll(List<byte[]> keys, long revUpperBound) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(byte[] key, byte[] value) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry getAndPut(byte[] key, byte[] value) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void remove(byte[] key) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull org.apache.ignite.internal.metastorage.server.Entry getAndRemove(byte[] key) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void removeAll(List<byte[]> keys) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public @NotNull Collection<org.apache.ignite.internal.metastorage.server.Entry> getAndRemoveAll(List<byte[]> keys) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean invoke(
                org.apache.ignite.internal.metastorage.server.Condition condition,
                Collection<org.apache.ignite.internal.metastorage.server.Operation> success,
                Collection<org.apache.ignite.internal.metastorage.server.Operation> failure
        ) {
            fail();

            return false;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.WatchEvent> watch(byte[] keyFrom, byte[] keyTo, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.WatchEvent> watch(byte[] key, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Cursor<org.apache.ignite.internal.metastorage.server.WatchEvent> watch(Collection<byte[]> keys, long rev) {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void compact() {
            fail();
        }
    }
}
