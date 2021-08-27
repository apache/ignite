/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.LoadSnapshotClosure;
import org.apache.ignite.raft.jraft.closure.SaveSnapshotClosure;
import org.apache.ignite.raft.jraft.closure.SynchronizedClosure;
import org.apache.ignite.raft.jraft.core.DefaultJRaftServiceFactory;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.option.CopyOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotExecutorOptions;
import org.apache.ignite.raft.jraft.rpc.GetFileRequestBuilder;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotExecutorImpl;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotMetaTable;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotWriter;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SnapshotExecutorTest extends BaseStorageTest {
    private SnapshotExecutorImpl executor;
    @Mock
    private NodeImpl node;
    @Mock
    private FSMCaller fSMCaller;
    @Mock
    private LogManager logManager;
    private Endpoint addr;
    @Mock
    private RpcContext asyncCtx;

    @Mock
    private RaftClientService raftClientService;
    private String uri;
    private final String hostPort = "localhost:8081";
    private final int readerId = 99;
    private CopyOptions copyOpts;
    private LocalSnapshotMetaTable table;
    private LocalSnapshotWriter writer;
    private LocalSnapshotReader reader;
    private RaftOptions raftOptions;
    @Mock
    private LocalSnapshotStorage snapshotStorage;
    private TimerManager timerManager;
    private NodeOptions options;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() throws Exception {
        timerManager = new TimerManager(5);
        raftOptions = new RaftOptions();
        writer = new LocalSnapshotWriter(path.toString(), snapshotStorage, raftOptions);
        reader = new LocalSnapshotReader(snapshotStorage, null, new Endpoint("localhost", 8081),
            raftOptions, path.toString());

        Mockito.lenient().when(snapshotStorage.open()).thenReturn(reader);
        Mockito.lenient().when(snapshotStorage.create(true)).thenReturn(writer);

        table = new LocalSnapshotMetaTable(raftOptions);
        table.addFile("testFile", raftOptions.getRaftMessagesFactory().localFileMeta().checksum("test").build());
        table.setMeta(raftOptions.getRaftMessagesFactory().snapshotMeta().lastIncludedIndex(1).lastIncludedTerm(1).build());
        uri = "remote://" + hostPort + "/" + readerId;
        copyOpts = new CopyOptions();

        Mockito.when(node.getRaftOptions()).thenReturn(new RaftOptions());
        options = new NodeOptions();
        options.setCommonExecutor(JRaftUtils.createExecutor("test-executor", Utils.cpus()));
        Mockito.when(node.getOptions()).thenReturn(options);
        Mockito.when(node.getRpcClientService()).thenReturn(raftClientService);
        Mockito.when(node.getTimerManager()).thenReturn(timerManager);
        Mockito.when(node.getServiceFactory()).thenReturn(new DefaultJRaftServiceFactory());
        executor = new SnapshotExecutorImpl();
        final SnapshotExecutorOptions opts = new SnapshotExecutorOptions();
        opts.setFsmCaller(fSMCaller);
        opts.setInitTerm(0);
        opts.setNode(node);
        opts.setLogManager(logManager);
        opts.setUri(path.toString());
        addr = new Endpoint("localhost", 8081);
        opts.setAddr(addr);
        assertTrue(executor.init(opts));
    }

    @AfterEach
    public void teardown() throws Exception {
        executor.shutdown();
        timerManager.shutdown();
        options.getCommonExecutor().shutdown();
        ExecutorServiceHelper.shutdownAndAwaitTermination(executorService);
    }

    @Test
    public void testInstallSnapshot() throws Exception {
        RaftMessagesFactory msgFactory = raftOptions.getRaftMessagesFactory();

        final RpcRequests.InstallSnapshotRequest irb = msgFactory.installSnapshotRequest()
            .groupId("test")
            .peerId(addr.toString())
            .serverId("localhost:8080")
            .uri("remote://localhost:8080/99")
            .term(0)
            .meta(msgFactory.snapshotMeta().lastIncludedIndex(1).lastIncludedTerm(2).build())
            .build();

        Mockito.when(raftClientService.connect(new Endpoint("localhost", 8080))).thenReturn(true);

        final CompletableFuture<Message> fut = new CompletableFuture<>();
        final GetFileRequestBuilder rb = msgFactory.getFileRequest()
            .readerId(99)
            .filename(Snapshot.JRAFT_SNAPSHOT_META_FILE)
            .count(Integer.MAX_VALUE)
            .offset(0)
            .readPartly(true);

        // Mock get metadata
        ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        Mockito.when(raftClientService.getFile(eq(new Endpoint("localhost", 8080)), eq(rb.build()),
                eq(copyOpts.getTimeoutMs()), argument.capture())).thenReturn(fut);

        Future<?> snapFut = Utils.runInThread(ForkJoinPool.commonPool(), () -> executor.installSnapshot(irb,
            msgFactory.installSnapshotResponse(), new RpcRequestClosure(asyncCtx, msgFactory)));

        assertTrue(TestUtils.waitForArgumentCapture(argument, 5_000));

        RpcResponseClosure<RpcRequests.GetFileResponse> closure = argument.getValue();
        final ByteBuffer metaBuf = table.saveToByteBufferAsRemote();
        closure.setResponse(msgFactory.getFileResponse().readSize(metaBuf.remaining()).eof(true)
            .data(new ByteString(metaBuf)).build());

        // Mock get file
        argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        rb.filename("testFile");
        rb.count(raftOptions.getMaxByteCountPerRpc());
        Mockito.when(raftClientService.getFile(eq(new Endpoint("localhost", 8080)), eq(rb.build()),
            eq(copyOpts.getTimeoutMs()), argument.capture())).thenReturn(fut);

        closure.run(Status.OK());

        assertTrue(TestUtils.waitForArgumentCapture(argument, 5_000));

        closure = argument.getValue();

        closure.setResponse(msgFactory.getFileResponse().readSize(100).eof(true)
            .data(new ByteString(new byte[100])).build());

        ArgumentCaptor<LoadSnapshotClosure> loadSnapshotArg = ArgumentCaptor.forClass(LoadSnapshotClosure.class);
        Mockito.when(fSMCaller.onSnapshotLoad(loadSnapshotArg.capture())).thenReturn(true);
        closure.run(Status.OK());

        assertTrue(TestUtils.waitForArgumentCapture(loadSnapshotArg, 5_000));

        final LoadSnapshotClosure done = loadSnapshotArg.getValue();
        final SnapshotReader reader = done.start();
        assertNotNull(reader);
        assertEquals(1, reader.listFiles().size());
        assertTrue(reader.listFiles().contains("testFile"));
        done.run(Status.OK());
        executor.join();

        assertTrue(snapFut.isDone());

        assertEquals(2, executor.getLastSnapshotTerm());
        assertEquals(1, executor.getLastSnapshotIndex());
    }

    @Test
    public void testInterruptInstalling() throws Exception {
        RaftMessagesFactory msgFactory = raftOptions.getRaftMessagesFactory();

        final RpcRequests.InstallSnapshotRequest irb = msgFactory.installSnapshotRequest()
            .groupId("test")
            .peerId(addr.toString())
            .serverId("localhost:8080")
            .uri("remote://localhost:8080/99")
            .term(0)
            .meta(msgFactory.snapshotMeta().lastIncludedIndex(1).lastIncludedTerm(1).build())
            .build();

        Mockito.lenient().when(raftClientService.connect(new Endpoint("localhost", 8080))).thenReturn(true);

        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest rb = msgFactory.getFileRequest()
            .readerId(99)
            .filename(Snapshot.JRAFT_SNAPSHOT_META_FILE)
            .count(Integer.MAX_VALUE)
            .offset(0)
            .readPartly(true)
            .build();

        // Mock get metadata
        final ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        Mockito.lenient().when(
            raftClientService.getFile(eq(new Endpoint("localhost", 8080)), eq(rb),
                eq(copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        executorService = singleThreadExecutor;
        Utils.runInThread(
            singleThreadExecutor,
            () -> executor.installSnapshot(irb, msgFactory.installSnapshotResponse(), new RpcRequestClosure(asyncCtx, msgFactory))
        );

        executor.interruptDownloadingSnapshots(1);
        executor.join();
        assertEquals(0, executor.getLastSnapshotTerm());
        assertEquals(0, executor.getLastSnapshotIndex());
    }

    @Test
    public void testDoSnapshot() throws Exception {
        Mockito.when(fSMCaller.getLastAppliedIndex()).thenReturn(1L);
        final ArgumentCaptor<SaveSnapshotClosure> saveSnapshotClosureArg = ArgumentCaptor
            .forClass(SaveSnapshotClosure.class);
        Mockito.when(fSMCaller.onSnapshotSave(saveSnapshotClosureArg.capture())).thenReturn(true);
        final SynchronizedClosure done = new SynchronizedClosure();
        executor.doSnapshot(done);
        final SaveSnapshotClosure closure = saveSnapshotClosureArg.getValue();
        assertNotNull(closure);
        closure.start(raftOptions.getRaftMessagesFactory().snapshotMeta().lastIncludedIndex(2).lastIncludedTerm(1).build());
        closure.run(Status.OK());
        done.await();
        executor.join();
        assertTrue(done.getStatus().isOk());
        assertEquals(1, executor.getLastSnapshotTerm());
        assertEquals(2, executor.getLastSnapshotIndex());
    }

    @Test
    public void testNotDoSnapshotWithIntervalDist() throws Exception {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setSnapshotLogIndexMargin(10);
        ExecutorService testExecutor = JRaftUtils.createExecutor("test-executor", Utils.cpus());
        executorService = testExecutor;
        nodeOptions.setCommonExecutor(testExecutor);
        Mockito.when(node.getOptions()).thenReturn(nodeOptions);
        Mockito.when(fSMCaller.getLastAppliedIndex()).thenReturn(1L);
        executor.doSnapshot(null);
        executor.join();

        assertEquals(0, executor.getLastSnapshotTerm());
        assertEquals(0, executor.getLastSnapshotIndex());
    }

    @Test
    public void testDoSnapshotWithIntervalDist() throws Exception {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setSnapshotLogIndexMargin(5);
        ExecutorService testExecutor = JRaftUtils.createExecutor("test-executor", Utils.cpus());
        executorService = testExecutor;
        nodeOptions.setCommonExecutor(testExecutor);
        Mockito.when(node.getOptions()).thenReturn(nodeOptions);
        Mockito.when(fSMCaller.getLastAppliedIndex()).thenReturn(6L);

        final ArgumentCaptor<SaveSnapshotClosure> saveSnapshotClosureArg = ArgumentCaptor
            .forClass(SaveSnapshotClosure.class);
        Mockito.when(fSMCaller.onSnapshotSave(saveSnapshotClosureArg.capture())).thenReturn(true);
        final SynchronizedClosure done = new SynchronizedClosure();
        executor.doSnapshot(done);
        final SaveSnapshotClosure closure = saveSnapshotClosureArg.getValue();
        assertNotNull(closure);
        closure.start(raftOptions.getRaftMessagesFactory().snapshotMeta().lastIncludedIndex(6).lastIncludedTerm(1).build());
        closure.run(Status.OK());
        done.await();
        executor.join();

        assertEquals(1, executor.getLastSnapshotTerm());
        assertEquals(6, executor.getLastSnapshotIndex());
    }
}
