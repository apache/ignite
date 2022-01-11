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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.RaftServerImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link InternalTable#scan(int, org.apache.ignite.internal.tx.InternalTransaction)}.
 */
@ExtendWith(MockitoExtension.class)
public class ItInternalTableScanTest {
    private static final TestScaleCubeClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    private static final String TEST_TABLE_NAME = "testTbl";

    /** Id for the test RAFT group. */
    public static final String RAFT_GRP_ID = "test_part_grp";

    /** Mock partition storage. */
    @Mock
    private PartitionStorage mockStorage;

    private ClusterService network;

    private RaftServer raftSrv;

    private TxManager txManager;

    /** Internal table to test. */
    private InternalTable internalTbl;

    /** Executor for raft group services. */
    ScheduledExecutorService executor;

    /**
     * Prepare test environment.
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
                NETWORK_FACTORY
        );

        network.start();

        raftSrv = new RaftServerImpl(network, FACTORY);

        raftSrv.start();

        String grpName = "test_part_grp";

        List<Peer> conf = List.of(new Peer(nodeNetworkAddress));

        mockStorage = mock(PartitionStorage.class);

        txManager = new TxManagerImpl(network, new HeapLockManager());

        txManager.start();

        IgniteUuid tblId = new IgniteUuid(UUID.randomUUID(), 0);

        raftSrv.startRaftGroup(
                grpName,
                new PartitionListener(tblId, new VersionedRowStore(mockStorage, txManager) {
                    @Override
                    protected Pair<BinaryRow, BinaryRow> versionedRow(@Nullable DataRow row, Timestamp timestamp) {
                        return new Pair<>(new ByteBufferRow(row.valueBytes()), null); // Return as is.
                    }
                }),
                conf
        );

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME));

        RaftGroupService raftGrpSvc = RaftGroupServiceImpl.start(
                RAFT_GRP_ID,
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
                tblId,
                Map.of(0, raftGrpSvc),
                1,
                NetworkAddress::toString,
                txManager,
                mock(TableStorage.class)
        );
    }

    /**
     * Cleanup previously started network and raft server.
     *
     * @throws Exception If failed to stop component.
     */
    @AfterEach
    public void tearDown() throws Exception {
        raftSrv.stopRaftGroup(RAFT_GRP_ID);

        if (raftSrv != null) {
            raftSrv.beforeNodeStop();
        }

        if (network != null) {
            network.beforeNodeStop();
        }

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        if (raftSrv != null) {
            raftSrv.stop();
        }

        if (network != null) {
            network.stop();
        }

        if (txManager != null) {
            txManager.stop();
        }
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by one row at a time.
     */
    @Test
    public void testOneRowScan() throws Exception {
        requestNtest(
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
        requestNtest(
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
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of negative requested amount of
     * items.
     *
     * @throws Exception If any.
     */
    @Test()
    public void testNegativeRequestedAmountScan() throws Exception {
        invalidRequestNtest(-1);
    }

    /**
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of zero requested amount of
     * items.
     *
     * @throws Exception If any.
     */
    @Test()
    public void testZeroRequestedAmountScan() throws Exception {
        invalidRequestNtest(0);
    }

    /**
     * Checks that exception from storage cursors has next properly propagates to subscriber.
     */
    @Test
    public void testExceptionRowScanCursorHasNext() throws Exception {
        // The latch that allows to await Subscriber.onComplete() before asserting test invariants
        // and avoids the race between closing the cursor and stopping the node.
        CountDownLatch subscriberFinishedLatch = new CountDownLatch(2);

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> true);

            when(cursor.next()).thenAnswer(hnInvocation -> {
                throw new NoSuchElementException("test");
            });

            doAnswer(
                    invocationClose -> {
                        subscriberFinishedLatch.countDown();
                        return null;
                    }
            ).when(cursor).close();

            return cursor;
        });

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {

            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                subscriberFinishedLatch.countDown();
            }

            @Override
            public void onComplete() {
                fail("Should never get here.");
            }
        });

        subscriberFinishedLatch.await();

        assertEquals(gotException.get().getCause().getClass(), NoSuchElementException.class);
    }

    /**
     * Checks that exception from storage cursor creation properly propagates to subscriber.
     */
    @Test
    public void testExceptionRowScan() throws Exception {
        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch gotExceptionLatch = new CountDownLatch(1);

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        when(mockStorage.scan(any())).thenThrow(new StorageException("Some storage exception"));

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {

            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(1);
            }

            @Override
            public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                gotExceptionLatch.countDown();
            }

            @Override
            public void onComplete() {
                fail("Should never get here.");
            }
        });

        gotExceptionLatch.await();

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
            @Override
            public void onSubscribe(Subscription subscription) {

            }

            @Override
            public void onNext(BinaryRow item) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });

        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch gotExceptionLatch = new CountDownLatch(1);

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        scan.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {

            }

            @Override
            public void onNext(BinaryRow item) {

            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                gotExceptionLatch.countDown();
            }

            @Override
            public void onComplete() {

            }
        });

        gotExceptionLatch.await();

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
     * @param submittedItems Items to be pushed by publisher.
     * @param reqAmount      Amount of rows to request at a time.
     * @throws Exception If Any.
     */
    private void requestNtest(List<DataRow> submittedItems, int reqAmount) throws Exception {
        AtomicInteger cursorTouchCnt = new AtomicInteger(0);

        List<BinaryRow> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> cursorTouchCnt.get() < submittedItems.size());

            when(cursor.next()).thenAnswer(ninvocation -> submittedItems.get(cursorTouchCnt.getAndIncrement()));

            return cursor;
        });

        // The latch that allows to await Subscriber.onError() before asserting test invariants.
        CountDownLatch subscriberAllDataAwaitLatch = new CountDownLatch(1);

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(reqAmount);
            }

            @Override
            public void onNext(BinaryRow item) {
                retrievedItems.add(item);

                if (retrievedItems.size() % reqAmount == 0) {
                    subscription.request(reqAmount);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                fail("onError call is not expected.");
            }

            @Override
            public void onComplete() {
                subscriberAllDataAwaitLatch.countDown();
            }
        });

        subscriberAllDataAwaitLatch.await();

        assertEquals(submittedItems.size(), retrievedItems.size());

        List<byte[]> expItems = submittedItems.stream().map(DataRow::valueBytes).collect(Collectors.toList());
        List<byte[]> gotItems = retrievedItems.stream().map(BinaryRow::bytes).collect(Collectors.toList());

        for (int i = 0; i < expItems.size(); i++) {
            assertTrue(Arrays.equals(expItems.get(i), gotItems.get(i)));
        }
    }

    /**
     * Checks whether {@link IllegalArgumentException} is thrown and inner storage cursor is closes in case of invalid requested amount of
     * items.
     *
     * @param reqAmount  Amount of rows to request at a time.
     * @throws Exception If Any.
     */
    private void invalidRequestNtest(int reqAmount) throws InterruptedException {
        // The latch that allows to await Subscriber.onComplete() before asserting test invariants
        // and avoids the race between closing the cursor and stopping the node.
        CountDownLatch subscriberFinishedLatch = new CountDownLatch(2);

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            doAnswer(
                    invocationClose -> {
                        subscriberFinishedLatch.countDown();
                        return null;
                    }
            ).when(cursor).close();

            when(cursor.hasNext()).thenAnswer(hnInvocation -> {
                throw new StorageException("test");
            });

            return cursor;
        });

        AtomicReference<Throwable> gotException = new AtomicReference<>();

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(reqAmount);
            }

            @Override
            public void onNext(BinaryRow item) {
                fail("Should never get here.");
            }

            @Override
            public void onError(Throwable throwable) {
                gotException.set(throwable);
                subscriberFinishedLatch.countDown();
            }

            @Override
            public void onComplete() {
                fail("Should never get here.");
            }
        });

        subscriberFinishedLatch.await();

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    throw gotException.get();
                }
        );
    }
}
