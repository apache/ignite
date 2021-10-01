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

package org.apache.ignite.distributed;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.RaftServerImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteUuidGenerator;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link InternalTable#scan(int, org.apache.ignite.tx.Transaction)}
 */
@ExtendWith(MockitoExtension.class)
public class ITInternalTableScanTest {
    /** */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** */
    private static final String TEST_TABLE_NAME = "testTbl";

    /** Mock partition storage. */
    @Mock
    private Storage mockStorage;

    /** */
    private ClusterService network;

    /** */
    private RaftServer raftSrv;

    /** Internal table to test. */
    private InternalTable internalTbl;

    /** Executor for raft group services. */
    ScheduledExecutorService executor;

    /**
     * Prepare test environment:
     * <ol>
     * <li>Start network node.</li>
     * <li>Start raft server.</li>
     * <li>Prepare partitioned raft group.</li>
     * <li>Prepare partitioned raft group service.</li>
     * <li>Prepare internal table as a test object.</li>
     * </ol>
     *
     * @throws Exception If any.
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        NetworkAddress nodeNetworkAddress = new NetworkAddress("localhost", 20_000);

        network = ClusterServiceTestUtils.clusterService(
            testInfo,
            20_000,
            new StaticNodeFinder(List.of(nodeNetworkAddress)),
            SERIALIZATION_REGISTRY,
            NETWORK_FACTORY
        );

        network.start();

        raftSrv = new RaftServerImpl(network, FACTORY);

        raftSrv.start();

        String grpName = "test_part_grp";

        List<Peer> conf = List.of(new Peer(nodeNetworkAddress));

        mockStorage = mock(Storage.class);

        raftSrv.startRaftGroup(
            grpName,
            new PartitionListener(mockStorage),
            conf
        );

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME));

        RaftGroupService raftGrpSvc = RaftGroupServiceImpl.start(
            grpName,
            network,
            FACTORY,
            10_000,
            conf,
            true,
            200,
            executor
        ).get(3, TimeUnit.SECONDS);

        internalTbl = new InternalTableImpl(
            TEST_TABLE_NAME,
            new IgniteUuidGenerator(UUID.randomUUID(), 0).randomUuid(),
            Map.of(0, raftGrpSvc),
            1
        );
    }

    /**
     * Cleanup previously started network and raft server.
     *
     * @throws Exception If failed to stop component.
     */
    @AfterEach
    public void tearDown() throws Exception {
        if (raftSrv != null)
            raftSrv.beforeNodeStop();

        if (network != null)
            network.beforeNodeStop();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        if (raftSrv != null)
            raftSrv.stop();

        if (network != null)
            network.stop();
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by one row at a time.
     */
    @Test
    public void testOneRowScan() throws Exception {
        requestNTest(
            List.of(
                prepareDataRow("key1", "val1"),
                prepareDataRow("key2", "val2")
            ),
            1);
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by multiple rows at a time.
     */
    @Test
    public void testMultipleRowScan() throws Exception {
        requestNTest(
            List.of(
                prepareDataRow("key1", "val1"),
                prepareDataRow("key2", "val2"),
                prepareDataRow("key3", "val3"),
                prepareDataRow("key4", "val4"),
                prepareDataRow("key5", "val5")
            ),
            2);
    }

    /**
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of invalid
     * requested amount of items.
     *
     * @throws Exception If any.
     */
    @Test()
    public void testInvalidRequestedAmountScan() throws Exception {
        AtomicBoolean cursorClosed = new AtomicBoolean(false);

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            doAnswer(
                invocationClose -> {
                    cursorClosed.set(true);
                    return null;
                }
            ).when(cursor).close();

            when(cursor.hasNext()).thenAnswer(hnInvocation -> {
                throw new StorageException("test");
            });

            return cursor;
        });

        for (long n : new long[] {-1, 0}) {
            AtomicReference<Throwable> gotException = new AtomicReference<>();

            cursorClosed.set(false);

            internalTbl.scan(0, null).subscribe(new Subscriber<>() {
                @Override public void onSubscribe(Subscription subscription) {
                    subscription.request(n);
                }

                @Override public void onNext(BinaryRow item) {
                    fail("Should never get here.");
                }

                @Override public void onError(Throwable throwable) {
                    gotException.set(throwable);
                }

                @Override public void onComplete() {
                    fail("Should never get here.");
                }
            });

            assertTrue(waitForCondition(() -> gotException.get() != null, 1_000));

            assertTrue(waitForCondition(cursorClosed::get, 1_000));

            assertThrows(
                IllegalArgumentException.class,
                () -> {
                    throw gotException.get();
                }
            );
        }
    }

    /**
     * Checks that exception from storage cursors has next properly propagates to subscriber.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15581")
    @Test
    public void testExceptionRowScanCursorHasNext() throws Exception {
        AtomicReference<Throwable> gotException = new AtomicReference<>();

        AtomicBoolean cursorClosed = new AtomicBoolean(false);

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> {
                throw new StorageException("test");
            });

            doAnswer(
                invocationClose -> {
                    cursorClosed.set(true);
                    return null;
                }
            ).when(cursor).close();

            return cursor;
        });

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {

            @Override public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override public void onError(Throwable throwable) {
                gotException.set(throwable);
            }

            @Override public void onComplete() {
                fail("Should never get here.");
            }
        });

        assertTrue(waitForCondition(() -> gotException.get() != null, 1_000));

        assertEquals(gotException.get().getCause().getClass(), StorageException.class);

        assertTrue(waitForCondition(cursorClosed::get, 1_000));
    }

    /**
     * Checks that exception from storage cursor creation properly propagates to subscriber.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15581")
    @Test
    public void testExceptionRowScan() throws Exception {
        AtomicReference<Throwable> gotException = new AtomicReference<>();

        when(mockStorage.scan(any())).thenThrow(new StorageException("Some storage exception"));

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {

            @Override public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override public void onError(Throwable throwable) {
                gotException.set(throwable);
            }

            @Override public void onComplete() {
                fail("Should never get here.");
            }
        });

        assertTrue(waitForCondition(() -> gotException.get() != null, 1_000));

        assertEquals(gotException.get().getCause().getClass(), StorageException.class);
    }


    /**
     * Checks that {@link IllegalArgumentException} is thrown in case of invalid partition.
     */
    @Test()
    public void testInvalidPartitionParameterScan() {
        assertThrows(
            IllegalArgumentException.class,
            () -> internalTbl.scan(-1, null)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> internalTbl.scan(1, null)
        );
    }

    /**
     * Checks that in case of second subscription {@link IllegalStateException} will be fires to onError.
     *
     * @throws Exception If any.
     */
    @Test
    public void testSecondSubscriptionFiresIllegalStateException() throws Exception {
        Flow.Publisher<BinaryRow> scan = internalTbl.scan(0, null);

        scan.subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) {

            }

            @Override public void onNext(BinaryRow item) {

            }

            @Override public void onError(Throwable throwable) {

            }

            @Override public void onComplete() {

            }
        });

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        scan.subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) {

            }

            @Override public void onNext(BinaryRow item) {

            }

            @Override public void onError(Throwable throwable) {
                gotException.set(throwable);
            }

            @Override public void onComplete() {

            }
        });

        assertTrue(waitForCondition(() -> gotException.get() != null, 1_000));

        assertEquals(gotException.get().getClass(), IllegalStateException.class);
    }

    /**
     * Checks that {@link NullPointerException} is thrown in case of null subscription.
     */
    @Test
    public void testNullPointerExceptionIsThrownInCaseOfNullSubscription() {
        assertThrows(
            NullPointerException.class,
            () -> internalTbl.scan(0, null).subscribe(null)
        );
    }

    /**
     * Helper method to convert key and value to {@link DataRow}.
     *
     * @param entryKey Key.
     * @param entryVal Value
     * @return {@link DataRow} based on given key and value.
     * @throws java.io.IOException If failed to close output stream that was used to convertation.
     */
    private static @NotNull DataRow prepareDataRow(@NotNull String entryKey,
        @NotNull String entryVal) throws IOException {
        byte[] keyBytes = ByteUtils.toBytes(entryKey);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            outputStream.write(keyBytes);
            outputStream.write(ByteUtils.toBytes(entryVal));

            return new SimpleDataRow(keyBytes, outputStream.toByteArray());
        }
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by reqAmount rows at a time.
     *
     * @param submittedItems Items to be pushed by ublisher.
     * @param reqAmount Amount of rows to request at a time.
     * @throws Exception If Any.
     */
    private void requestNTest(List<DataRow> submittedItems, int reqAmount) throws Exception {
        AtomicInteger cursorTouchCnt = new AtomicInteger(0);

        List<BinaryRow> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> cursorTouchCnt.get() < submittedItems.size());

            when(cursor.next()).thenAnswer(nInvocation -> submittedItems.get(cursorTouchCnt.getAndIncrement()));

            return cursor;
        });

        AtomicBoolean noMoreData = new AtomicBoolean(false);

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(reqAmount);
            }

            @Override public void onNext(BinaryRow item) {
                retrievedItems.add(item);

                if (retrievedItems.size() % reqAmount == 0)
                    subscription.request(reqAmount);
            }

            @Override public void onError(Throwable throwable) {
                fail("onError call is not expected.");
            }

            @Override public void onComplete() {
                noMoreData.set(true);
            }
        });

        assertTrue(waitForCondition(() -> retrievedItems.size() == submittedItems.size(), 2_000));

        List<byte[]> expItems = submittedItems.stream().map(DataRow::valueBytes).collect(Collectors.toList());
        List<byte[]> gotItems = retrievedItems.stream().map(BinaryRow::bytes).collect(Collectors.toList());

        for (int i = 0; i < expItems.size(); i++)
            assertTrue(Arrays.equals(expItems.get(i), gotItems.get(i)));

        assertTrue(noMoreData.get(), "More data is not expected.");
    }
}
