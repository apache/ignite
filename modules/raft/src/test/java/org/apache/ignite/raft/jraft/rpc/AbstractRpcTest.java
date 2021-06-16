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
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.raft.jraft.test.TestUtils.INIT_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
public abstract class AbstractRpcTest {
    protected Endpoint endpoint;

    private final List<RpcServer<?>> servers = new ArrayList<>();

    private final List<RpcClient> clients = new ArrayList<>();

    @BeforeEach
    public void setup() {
        endpoint = new Endpoint(TestUtils.getLocalAddress(), INIT_PORT);

        RpcServer<?> server = createServer(endpoint);
        server.registerProcessor(new Request1RpcProcessor());
        server.registerProcessor(new Request2RpcProcessor());
        server.init(null);

        servers.add(server);
    }

    @AfterEach
    public void tearDown() {
        clients.forEach(RpcClient::shutdown);
        servers.forEach(RpcServer::shutdown);
    }

    /**
     * @param endpoint The endpoint.
     * @return The server.
     */
    public abstract RpcServer<?> createServer(Endpoint endpoint);

    /**
     * @return The client.
     */
    private RpcClient createClient() {
        RpcClient client = createClient0();

        clients.add(client);

        return client;
    }

    public abstract RpcClient createClient0();

    @Test
    public void testConnection() {
        RpcClient client = createClient();

        assertTrue(client.checkConnection(endpoint));
    }

    @Test
    public void testSyncProcessing() throws Exception {
        RpcClient client = createClient();
        Response1 resp1 = (Response1) client.invokeSync(endpoint, new Request1(), new InvokeContext(), 5000);
        assertNotNull(resp1);

        Response2 resp2 = (Response2) client.invokeSync(endpoint, new Request2(), new InvokeContext(), 5000);
        assertNotNull(resp2);
    }

    @Test
    public void testAsyncProcessing() throws Exception {
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
    }

    @Test
    public void testDisconnect() {
        RpcClient client1 = createClient();
        RpcClient client2 = createClient();

        assertTrue(client1.checkConnection(endpoint));
        assertTrue(client2.checkConnection(endpoint));

        servers.get(0).shutdown();

        assertTrue(waitForTopology(client1, 2, 5_000));
        assertTrue(waitForTopology(client2, 2, 5_000));

        assertFalse(client1.checkConnection(endpoint));
        assertFalse(client2.checkConnection(endpoint));
    }

    @Test
    public void testRecordedSync() throws Exception {
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
    }

    @Test
    public void testRecordedSyncTimeout() {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint));

        try {
            Request1 request = new Request1();
            request.val = 10_000;
            client1.invokeSync(endpoint, request, 500);

            fail();
        }
        catch (Exception ignored) {
            // Expected.
        }

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(1, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);
    }

    @Test
    public void testRecordedAsync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint));

        CountDownLatch l = new CountDownLatch(2);

        client1.invokeAsync(endpoint, new Request1(), null, (result, err) -> l.countDown(), 500);
        client1.invokeAsync(endpoint, new Request2(), null, (result, err) -> l.countDown(), 500);

        l.await();

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(4, recorded.size());
    }

    @Test
    public void testRecordedAsyncTimeout() {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint));

        try {
            Request1 request = new Request1();
            request.val = 10_000;
            CompletableFuture<Object> fut = new CompletableFuture<>();

            client1.invokeAsync(endpoint, request, null, (result, err) -> {
                if (err == null)
                    fut.complete(result);
                else
                    fut.completeExceptionally(err);
            }, 500);

            fut.get(); // Should throw timeout exception.

            fail();
        }
        catch (Exception ignored) {
            // Expected.
        }

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(1, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);
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
    }

    @Test
    public void testBlockedAsync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.blockMessages((msg, id) -> msg instanceof Request1);

        assertTrue(client1.checkConnection(endpoint));

        CompletableFuture<Object> resp = new CompletableFuture<>();

        client1.invokeAsync(endpoint, new Request1(), (result, err) -> resp.complete(result), 30_000);

        Thread.sleep(500);

        Queue<Object[]> msgs = client1.blockedMessages();

        assertEquals(1, msgs.size());

        assertFalse(resp.isDone());

        client1.stopBlock();

        resp.get(5_000, TimeUnit.MILLISECONDS);
    }

    /** */
    private static class Request1RpcProcessor implements RpcProcessor<Request1> {
        /** {@inheritDoc} */
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

        /** {@inheritDoc} */
        @Override public String interest() {
            return Request1.class.getName();
        }
    }

    /** */
    private static class Request2RpcProcessor implements RpcProcessor<Request2> {
        /** {@inheritDoc} */
        @Override public void handleRequest(RpcContext rpcCtx, Request2 request) {
            Response2 resp2 = new Response2();
            resp2.val = request.val + 1;
            rpcCtx.sendResponse(resp2);
        }

        /** {@inheritDoc} */
        @Override public String interest() {
            return Request2.class.getName();
        }
    }

    /** */
    private static class Request1 implements Message {
        /** */
        int val;
    }

    /** */
    private static class Request2 implements Message {
        /** */
        int val;
    }

    /** */
    private static class Response1 implements Message {
        /** */
        int val;
    }

    /** */
    private static class Response2 implements Message {
        /** */
        int val;
    }

    /**
     * @param client The client.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     */
    protected abstract boolean waitForTopology(RpcClient client, int expected, long timeout);
}
