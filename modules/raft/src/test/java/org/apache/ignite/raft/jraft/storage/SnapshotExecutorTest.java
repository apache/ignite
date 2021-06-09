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
import java.util.concurrent.Executors;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.LoadSnapshotClosure;
import org.apache.ignite.raft.jraft.closure.SaveSnapshotClosure;
import org.apache.ignite.raft.jraft.closure.SynchronizedClosure;
import org.apache.ignite.raft.jraft.core.DefaultJRaftServiceFactory;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.option.CopyOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotExecutorOptions;
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
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;

@RunWith(value = MockitoJUnitRunner.class)
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

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.timerManager = new TimerManager(5);
        this.raftOptions = new RaftOptions();
        this.writer = new LocalSnapshotWriter(this.path, this.snapshotStorage, this.raftOptions);
        this.reader = new LocalSnapshotReader(this.snapshotStorage, null, new Endpoint("localhost", 8081),
            this.raftOptions, this.path);

        Mockito.lenient().when(this.snapshotStorage.open()).thenReturn(this.reader);
        Mockito.lenient().when(this.snapshotStorage.create(true)).thenReturn(this.writer);

        this.table = new LocalSnapshotMetaTable(this.raftOptions);
        this.table.addFile("testFile", LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("test").build());
        this.table.setMeta(RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(1).setLastIncludedTerm(1).build());
        this.uri = "remote://" + this.hostPort + "/" + this.readerId;
        this.copyOpts = new CopyOptions();

        Mockito.when(this.node.getRaftOptions()).thenReturn(new RaftOptions());
        NodeOptions options = new NodeOptions();
        options.setCommonExecutor(JRaftUtils.createExecutor("test-executor", Utils.cpus()));
        Mockito.when(this.node.getOptions()).thenReturn(options);
        Mockito.when(this.node.getRpcClientService()).thenReturn(this.raftClientService);
        Mockito.when(this.node.getTimerManager()).thenReturn(this.timerManager);
        Mockito.when(this.node.getServiceFactory()).thenReturn(new DefaultJRaftServiceFactory());
        this.executor = new SnapshotExecutorImpl();
        final SnapshotExecutorOptions opts = new SnapshotExecutorOptions();
        opts.setFsmCaller(this.fSMCaller);
        opts.setInitTerm(0);
        opts.setNode(this.node);
        opts.setLogManager(this.logManager);
        opts.setUri(this.path);
        this.addr = new Endpoint("localhost", 8081);
        opts.setAddr(this.addr);
        assertTrue(this.executor.init(opts));
    }

    @Override
    @After
    public void teardown() throws Exception {
        this.executor.shutdown();
        super.teardown();
        this.timerManager.shutdown();
    }

    @Test
    public void testInstallSnapshot() throws Exception {
        final RpcRequests.InstallSnapshotRequest.Builder irb = RpcRequests.InstallSnapshotRequest.newBuilder();
        irb.setGroupId("test");
        irb.setPeerId(this.addr.toString());
        irb.setServerId("localhost:8080");
        irb.setUri("remote://localhost:8080/99");
        irb.setTerm(0);
        irb.setMeta(RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(1).setLastIncludedTerm(2).build());

        Mockito.when(this.raftClientService.connect(new Endpoint("localhost", 8080))).thenReturn(true);

        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename(Snapshot.JRAFT_SNAPSHOT_META_FILE).setCount(Integer.MAX_VALUE).setOffset(0)
            .setReadPartly(true);

        //mock get metadata
        ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        Mockito.when(
            this.raftClientService.getFile(eq(new Endpoint("localhost", 8080)), eq(rb.build()),
                eq(this.copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);

        Utils.runInThread(Executors.newSingleThreadExecutor(), new Runnable() {
            @Override
            public void run() {
                SnapshotExecutorTest.this.executor.installSnapshot(irb.build(), RpcRequests.InstallSnapshotResponse
                    .newBuilder(), new RpcRequestClosure(SnapshotExecutorTest.this.asyncCtx));
            }
        });

        Thread.sleep(500);
        RpcResponseClosure<RpcRequests.GetFileResponse> closure = argument.getValue();
        final ByteBuffer metaBuf = this.table.saveToByteBufferAsRemote();
        closure.setResponse(RpcRequests.GetFileResponse.newBuilder().setReadSize(metaBuf.remaining()).setEof(true)
            .setData(new ByteString(metaBuf)).build());

        //mock get file
        argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        rb.setFilename("testFile");
        rb.setCount(this.raftOptions.getMaxByteCountPerRpc());
        Mockito.when(
            this.raftClientService.getFile(eq(new Endpoint("localhost", 8080)), eq(rb.build()),
                eq(this.copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);

        closure.run(Status.OK());

        Thread.sleep(500);
        closure = argument.getValue();
        closure.setResponse(RpcRequests.GetFileResponse.newBuilder().setReadSize(100).setEof(true)
            .setData(new ByteString(new byte[100])).build());

        final ArgumentCaptor<LoadSnapshotClosure> loadSnapshotArg = ArgumentCaptor.forClass(LoadSnapshotClosure.class);
        Mockito.when(this.fSMCaller.onSnapshotLoad(loadSnapshotArg.capture())).thenReturn(true);
        closure.run(Status.OK());
        Thread.sleep(500);
        final LoadSnapshotClosure done = loadSnapshotArg.getValue();
        final SnapshotReader reader = done.start();
        assertNotNull(reader);
        assertEquals(1, reader.listFiles().size());
        assertTrue(reader.listFiles().contains("testFile"));
        done.run(Status.OK());
        this.executor.join();
        assertEquals(2, this.executor.getLastSnapshotTerm());
        assertEquals(1, this.executor.getLastSnapshotIndex());
    }

    @Test
    public void testInterruptInstalling() throws Exception {
        final RpcRequests.InstallSnapshotRequest.Builder irb = RpcRequests.InstallSnapshotRequest.newBuilder();
        irb.setGroupId("test");
        irb.setPeerId(this.addr.toString());
        irb.setServerId("localhost:8080");
        irb.setUri("remote://localhost:8080/99");
        irb.setTerm(0);
        irb.setMeta(RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(1).setLastIncludedTerm(1).build());

        Mockito.lenient().when(this.raftClientService.connect(new Endpoint("localhost", 8080))).thenReturn(true);

        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename(Snapshot.JRAFT_SNAPSHOT_META_FILE).setCount(Integer.MAX_VALUE).setOffset(0)
            .setReadPartly(true);

        //mock get metadata
        final ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        Mockito.lenient().when(
            this.raftClientService.getFile(eq(new Endpoint("localhost", 8080)), eq(rb.build()),
                eq(this.copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);
        Utils.runInThread(Executors.newSingleThreadExecutor(), new Runnable() {
            @Override
            public void run() {
                SnapshotExecutorTest.this.executor.installSnapshot(irb.build(), RpcRequests.InstallSnapshotResponse
                    .newBuilder(), new RpcRequestClosure(SnapshotExecutorTest.this.asyncCtx));

            }
        });

        this.executor.interruptDownloadingSnapshots(1);
        this.executor.join();
        assertEquals(0, this.executor.getLastSnapshotTerm());
        assertEquals(0, this.executor.getLastSnapshotIndex());
    }

    @Test
    public void testDoSnapshot() throws Exception {
        Mockito.when(this.fSMCaller.getLastAppliedIndex()).thenReturn(1L);
        final ArgumentCaptor<SaveSnapshotClosure> saveSnapshotClosureArg = ArgumentCaptor
            .forClass(SaveSnapshotClosure.class);
        Mockito.when(this.fSMCaller.onSnapshotSave(saveSnapshotClosureArg.capture())).thenReturn(true);
        final SynchronizedClosure done = new SynchronizedClosure();
        this.executor.doSnapshot(done);
        final SaveSnapshotClosure closure = saveSnapshotClosureArg.getValue();
        assertNotNull(closure);
        closure.start(RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(2).setLastIncludedTerm(1).build());
        closure.run(Status.OK());
        done.await();
        this.executor.join();
        assertTrue(done.getStatus().isOk());
        assertEquals(1, this.executor.getLastSnapshotTerm());
        assertEquals(2, this.executor.getLastSnapshotIndex());
    }

    @Test
    public void testNotDoSnapshotWithIntervalDist() throws Exception {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setSnapshotLogIndexMargin(10);
        nodeOptions.setCommonExecutor(JRaftUtils.createExecutor("test-executor", Utils.cpus()));
        Mockito.when(this.node.getOptions()).thenReturn(nodeOptions);
        Mockito.when(this.fSMCaller.getLastAppliedIndex()).thenReturn(1L);
        this.executor.doSnapshot(null);
        this.executor.join();

        assertEquals(0, this.executor.getLastSnapshotTerm());
        assertEquals(0, this.executor.getLastSnapshotIndex());

    }

    @Test
    public void testDoSnapshotWithIntervalDist() throws Exception {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setSnapshotLogIndexMargin(5);
        nodeOptions.setCommonExecutor(JRaftUtils.createExecutor("test-executor", Utils.cpus()));
        Mockito.when(this.node.getOptions()).thenReturn(nodeOptions);
        Mockito.when(this.fSMCaller.getLastAppliedIndex()).thenReturn(6L);

        final ArgumentCaptor<SaveSnapshotClosure> saveSnapshotClosureArg = ArgumentCaptor
            .forClass(SaveSnapshotClosure.class);
        Mockito.when(this.fSMCaller.onSnapshotSave(saveSnapshotClosureArg.capture())).thenReturn(true);
        final SynchronizedClosure done = new SynchronizedClosure();
        this.executor.doSnapshot(done);
        final SaveSnapshotClosure closure = saveSnapshotClosureArg.getValue();
        assertNotNull(closure);
        closure.start(RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(6).setLastIncludedTerm(1).build());
        closure.run(Status.OK());
        done.await();
        this.executor.join();

        assertEquals(1, this.executor.getLastSnapshotTerm());
        assertEquals(6, this.executor.getLastSnapshotIndex());

    }
}
