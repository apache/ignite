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
package org.apache.ignite.raft.jraft.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.raft.jraft.test.TestUtils.INIT_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public abstract class AbstractRpcTest {
    protected Endpoint endpoint;
    protected List<RpcServer> servers = new ArrayList<>();

    @Before
    public void setup() {
        endpoint = new Endpoint(TestUtils.getMyIp(), INIT_PORT);
        RpcServer server = createServer(endpoint);
        server.registerProcessor(new Request1RpcProcessor());
        server.registerProcessor(new Request2RpcProcessor());
        server.init(null);

        servers.add(server);
    }

    @After
    public void teardown() {
        for (RpcServer server : servers) {
            server.shutdown();
        }
    }

    /**
     * @param endpoint The endpoint.
     * @return The server.
     */
    public abstract RpcServer createServer(Endpoint endpoint);

    /**
     * @return The client.
     */
    public abstract RpcClient createClient();

    @Test
    public void testConnection() {
        RpcClient client = createClient();

        assertTrue(client.checkConnection(endpoint));

        client.shutdown();
    }

    @Test
    public void testSyncProcessing() throws RemotingException, InterruptedException {
        RpcClient client = createClient();
        Response1 resp1 = (Response1) client.invokeSync(endpoint, new Request1(), new InvokeContext(), 5000);
        assertNotNull(resp1);

        Response2 resp2 = (Response2) client.invokeSync(endpoint, new Request2(), new InvokeContext(), 5000);
        assertNotNull(resp2);

        client.shutdown();
    }

    @Test
    public void testAsyncProcessing() throws RemotingException, InterruptedException {
        RpcClient client = createClient();

        CountDownLatch l1 = new CountDownLatch(1);
        AtomicReference<Response1> resp1 = new AtomicReference<>();
        client.invokeAsync(endpoint, new Request1(), new InvokeContext(), (result, err) -> {
            resp1.set((Response1) result);
            l1.countDown();
        }, 5000);
        l1.await(5_000, TimeUnit.MILLISECONDS);
        assertNotNull(resp1);

        CountDownLatch l2 = new CountDownLatch(1);
        AtomicReference<Response2> resp2 = new AtomicReference<>();
        client.invokeAsync(endpoint, new Request2(), new InvokeContext(), (result, err) -> {
            resp2.set((Response2) result);
            l2.countDown();
        }, 5000);
        l2.await(5_000, TimeUnit.MILLISECONDS);
        assertNotNull(resp2);

        client.shutdown();
    }

    @Test
    public void testDisconnect() throws Exception {
        RpcClient client1 = createClient();
        RpcClient client2 = createClient();

        try {
            assertTrue(client1.checkConnection(endpoint));
            assertTrue(client2.checkConnection(endpoint));

            servers.get(0).shutdown();

            assertTrue(waitForTopology(client1, 2, 5_000));
            assertTrue(waitForTopology(client2, 2, 5_000));

            assertFalse(client1.checkConnection(endpoint));
            assertFalse(client2.checkConnection(endpoint));
        }
        finally {
            client1.shutdown();
            client2.shutdown();
        }
    }

    @Test
    public void testRecordedSync() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint));

        Response1 resp1 = (Response1) client1.invokeSync(endpoint, new Request1(), 500);
        Response2 resp2 = (Response2) client1.invokeSync(endpoint, new Request2(), 500);

        assertNotNull(resp1);
        assertNotNull(resp2);

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(4, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);
        assertTrue(recorded.poll()[0] instanceof Response1);
        assertTrue(recorded.poll()[0] instanceof Request2);
        assertTrue(recorded.poll()[0] instanceof Response2);

        client1.shutdown();
    }

    @Test
    public void testRecordedSyncTimeout() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint));

        try {
            Request1 request = new Request1();
            request.val = 10_000;
            client1.invokeSync(endpoint, request, 500);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(1, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);

        client1.shutdown();
    }

    @Test
    public void testRecordedAsync() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint));

        CountDownLatch l = new CountDownLatch(2);

        client1.invokeAsync(endpoint, new Request1(), null, (result, err) -> l.countDown(), 500);
        client1.invokeAsync(endpoint, new Request2(), null, (result, err) -> l.countDown(), 500);

        l.await();

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(4, recorded.size());

        client1.shutdown();
    }

    @Test
    public void testRecordedAsyncTimeout() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint));

        try {
            Request1 request = new Request1();
            request.val = 10_000;
            CompletableFuture fut = new CompletableFuture<>();

            client1.invokeAsync(endpoint, request, null, (result, err) -> {
                if (err == null)
                    fut.complete(result);
                else
                    fut.completeExceptionally(err);
            }, 500);

            fut.get(); // Should throw timeout exception.

            fail();
        }
        catch (Exception e) {
            // Expected.
        }

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(1, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);

        client1.shutdown();
    }

    @Test
    public void testBlockedSync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.blockMessages((msg, id) -> msg instanceof Request1);

        assertTrue(client1.checkConnection(endpoint));

        Response2 resp2 = (Response2) client1.invokeSync(endpoint, new Request2(), 500);

        assertEquals(1, resp2.val);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Future<Response1> resp = Utils.runInThread(executorService,
            () -> (Response1) client1.invokeSync(endpoint, new Request1(), 30_000));

        Thread.sleep(500);

        Queue<Object[]> msgs = client1.blockedMessages();

        assertEquals(1, msgs.size());

        assertFalse(resp.isDone());

        client1.stopBlock();

        resp.get(5_000, TimeUnit.MILLISECONDS);

        client1.shutdown();
    }

    @Test
    public void testBlockedAsync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.blockMessages((msg, id) -> msg instanceof Request1);

        assertTrue(client1.checkConnection(endpoint));

        CompletableFuture resp = new CompletableFuture();

        client1.invokeAsync(endpoint, new Request1(), new InvokeCallback() {
            @Override public void complete(Object result, Throwable err) {
                resp.complete(result);
            }
        }, 30_000);

        Thread.sleep(500);

        Queue<Object[]> msgs = client1.blockedMessages();

        assertEquals(1, msgs.size());

        assertFalse(resp.isDone());

        client1.stopBlock();

        resp.get(5_000, TimeUnit.MILLISECONDS);

        client1.shutdown();
    }

    protected static class Request1RpcProcessor implements RpcProcessor<Request1> {
        @Override public void handleRequest(RpcContext rpcCtx, Request1 request) {
            if (request.val == 10_000)
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

            Response1 resp1 = new Response1();
            resp1.val = request.val + 1;
            rpcCtx.sendResponse(resp1);
        }

        @Override public String interest() {
            return Request1.class.getName();
        }
    }

    protected static class Request2RpcProcessor implements RpcProcessor<Request2> {
        @Override public void handleRequest(RpcContext rpcCtx, Request2 request) {
            Response2 resp2 = new Response2();
            resp2.val = request.val + 1;
            rpcCtx.sendResponse(resp2);
        }

        @Override public String interest() {
            return Request2.class.getName();
        }
    }

    private static class Request1 implements Message {
        int val;
    }

    private static class Request2 implements Message {
        int val;
    }

    private static class Response1 implements Message {
        int val;
    }

    private static class Response2 implements Message {
        int val;
    }

    /**
     * @param client The client.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     */
    protected abstract boolean waitForTopology(RpcClient client, int expected, long timeout);
}
