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
package org.apache.ignite.raft.jraft.storage.snapshot.remote;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.CopyOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(value = MockitoJUnitRunner.class)
public class CopySessionTest {
    private CopySession session;
    @Mock
    private RaftClientService rpcService;
    private RpcRequests.GetFileRequest.Builder rb;
    private final Endpoint address = new Endpoint("localhost", 8081);
    private CopyOptions copyOpts;
    private RaftOptions raftOpts;
    private NodeOptions nodeOptions;
    private TimerManager timerManager;

    @Before
    public void setup() {
        this.timerManager = new TimerManager(5);
        this.copyOpts = new CopyOptions();
        this.rb = RpcRequests.GetFileRequest.newBuilder();
        this.rb.setReaderId(99);
        this.rb.setFilename("data");
        this.raftOpts = new RaftOptions();
        this.nodeOptions = new NodeOptions();
        this.nodeOptions.setCommonExecutor(Executors.newSingleThreadExecutor());
        this.session = new CopySession(rpcService, timerManager, null, raftOpts, this.nodeOptions, rb, address);
        this.session.setCopyOptions(copyOpts);
    }

    @After
    public void teardown() {
        Utils.closeQuietly(this.session);
        this.timerManager.shutdown();
    }

    @Test
    public void testSendNextRpc() {
        final int maxCount = this.raftOpts.getMaxByteCountPerRpc();
        sendNextRpc(maxCount);
    }

    @Test
    public void testSendNextRpcWithBuffer() {
        session.setDestBuf(ByteBufferCollector.allocate(1));
        final int maxCount = Integer.MAX_VALUE;
        sendNextRpc(maxCount);
    }

    @Test
    public void testOnRpcReturnedEOF() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                try {
                    //test join, should return
                    session.join();
                    latch.countDown();
                }
                catch (final InterruptedException e) {
                    // No-op.
                }
            }
        }.start();
        assertNull(this.session.getRpcCall());
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.setDestBuf(bufRef);

        this.session.onRpcReturned(Status.OK(), RpcRequests.GetFileResponse.newBuilder().setReadSize(100).setEof(true)
            .setData(new ByteString(new byte[100])).build());
        assertEquals(100, bufRef.capacity());
        //should be flip
        assertEquals(0, bufRef.getBuffer().position());
        assertEquals(100, bufRef.getBuffer().remaining());

        assertNull(this.session.getRpcCall());
        latch.await();
    }

    @Test
    public void testOnRpcReturnedOK() {
        assertNull(this.session.getRpcCall());
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.setDestBuf(bufRef);

        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(Integer.MAX_VALUE).setOffset(100).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), session.getDone()))
            .thenReturn(future);

        this.session.onRpcReturned(Status.OK(), RpcRequests.GetFileResponse.newBuilder().setReadSize(100).setEof(false)
            .setData(new ByteString(new byte[100])).build());
        assertEquals(100, bufRef.capacity());
        assertEquals(100, bufRef.getBuffer().position());

        assertNotNull(this.session.getRpcCall());
        //send next request
        assertSame(future, this.session.getRpcCall());
    }

    @Test
    public void testOnRpcReturnedRetry() throws Exception {
        assertNull(this.session.getTimer());
        assertNull(this.session.getRpcCall());
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.setDestBuf(bufRef);

        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(Integer.MAX_VALUE).setOffset(0).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), session.getDone()))
            .thenReturn(future);

        this.session.onRpcReturned(new Status(RaftError.EINTR, "test"), null);
        assertNotNull(this.session.getTimer());
        Thread.sleep(this.copyOpts.getRetryIntervalMs() + 100);
        assertNotNull(this.session.getRpcCall());
        assertSame(future, this.session.getRpcCall());
        assertNull(this.session.getTimer());
    }

    private void sendNextRpc(int maxCount) {
        assertNull(this.session.getRpcCall());
        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(maxCount).setOffset(0).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), session.getDone()))
            .thenReturn(future);
        this.session.sendNextRpc();
        assertNotNull(this.session.getRpcCall());
        assertSame(future, this.session.getRpcCall());
    }
}
