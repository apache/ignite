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

package org.apache.ignite.internal.client.thin.events;

import java.io.DataInputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.events.ConnectionClosedEvent;
import org.apache.ignite.client.events.ConnectionDescription;
import org.apache.ignite.client.events.ConnectionEventListener;
import org.apache.ignite.client.events.HandshakeStartEvent;
import org.apache.ignite.client.events.HandshakeSuccessEvent;
import org.apache.ignite.client.events.RequestEvent;
import org.apache.ignite.client.events.RequestEventListener;
import org.apache.ignite.client.events.RequestFailEvent;
import org.apache.ignite.client.events.RequestStartEvent;
import org.apache.ignite.client.events.RequestSuccessEvent;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Test;

import static org.apache.ignite.internal.client.thin.ProtocolVersion.LATEST_VER;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.util.CommonUtils.HANDSHAKE;
import static org.apache.ignite.internal.util.CommonUtils.THIN_CLIENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

/** Tests server-assigned connection IDs in thin client events and diagnostics. */
public class IgniteClientConnectionIdTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** Tests IDs in events, including an asynchronous failure delivered after reconnection. */
    @Test
    public void testConnectionIdAfterReconnect() throws Exception {
        List<ConnectionDescription> starts = new CopyOnWriteArrayList<>();
        List<ConnectionDescription> connections = new CopyOnWriteArrayList<>();
        List<ConnectionDescription> closed = new CopyOnWriteArrayList<>();
        List<RequestEvent> requests = new CopyOnWriteArrayList<>();
        List<String> msgs = new CopyOnWriteArrayList<>();
        BlockingQueue<Runnable> completions = new LinkedBlockingQueue<>();
        ListeningTestLogger clientLog = new ListeningTestLogger(getClientConfiguration().getLogger());

        clientLog.registerListener(msgs::add);

        ClientConfiguration cfg = getClientConfiguration(grid(0))
            .setPartitionAwarenessEnabled(false)
            .setClusterDiscoveryEnabled(false)
            .setLogger(clientLog)
            .setAsyncContinuationExecutor(completions::add)
            .setEventListeners(new ConnectionEventListener() {
                @Override public void onHandshakeStart(HandshakeStartEvent event) {
                    starts.add(event.connectionDescription());
                }

                @Override public void onHandshakeSuccess(HandshakeSuccessEvent event) {
                    connections.add(event.connectionDescription());
                }

                @Override public void onConnectionClosed(ConnectionClosedEvent event) {
                    closed.add(event.connectionDescription());
                }
            }, new RequestEventListener() {
                @Override public void onRequestStart(RequestStartEvent event) {
                    requests.add(event);
                }

                @Override public void onRequestSuccess(RequestSuccessEvent event) {
                    requests.add(event);
                }

                @Override public void onRequestFail(RequestFailEvent event) {
                    requests.add(event);
                }
            });

        try (IgniteClient client = Ignition.startClient(cfg)) {
            assertEquals(1, connections.size());

            ConnectionDescription oldConn = connections.get(0);
            Long oldId = oldConn.connectionId();

            assertConnectionId(oldConn);
            assertTrue(msgs.stream().anyMatch(msg -> msg.startsWith("Handshake succeeded")
                && msg.contains("connectionId=" + oldId)));

            requests.clear();

            client.cacheNames();

            assertEquals(2, requests.size());
            assertTrue(requests.get(0) instanceof RequestStartEvent);
            assertTrue(requests.get(1) instanceof RequestSuccessEvent);
            assertEquals(requests.get(0).requestId(), requests.get(1).requestId());

            for (RequestEvent evt : requests)
                assertEquals(oldId, evt.connectionDescription().connectionId());

            IgniteClientFuture<Void> failed = client.cache("non-existent").putAsync(1, 1);
            Runnable completion = completions.poll(10, TimeUnit.SECONDS);

            assertNotNull(completion);
            assertFalse(failed.isDone());

            RequestEvent failedStart = requests.get(requests.size() - 1);

            assertTrue(failedStart instanceof RequestStartEvent);

            ClientProcessorMXBean mxBean = getMxBean(grid(0).name(), "Clients",
                ClientListenerProcessor.class, ClientProcessorMXBean.class);

            assertTrue(mxBean.dropConnection(oldId));
            assertTrue(GridTestUtils.waitForCondition(() -> closed.size() == 1, 10_000));
            assertEquals(oldId, closed.get(0).connectionId());

            client.cacheNames();

            assertEquals(2, connections.size());

            ConnectionDescription newConn = connections.get(1);

            assertConnectionId(newConn);
            assertNotEquals(oldId, newConn.connectionId());
            assertEquals(newConn.connectionId(), requests.get(requests.size() - 1).connectionDescription().connectionId());

            completion.run();

            GridTestUtils.assertThrowsAnyCause(log, failed::get, ClientException.class, null);

            RequestEvent failedEvt = requests.get(requests.size() - 1);

            assertTrue(failedEvt instanceof RequestFailEvent);
            assertEquals(failedStart.requestId(), failedEvt.requestId());
            assertEquals(oldId, failedEvt.connectionDescription().connectionId());
            assertEquals(oldId, oldConn.connectionId());
            assertEquals(oldId, closed.get(0).connectionId());
            assertTrue(msgs.stream().anyMatch(msg -> msg.startsWith("Client disconnected")
                && msg.contains("connectionId=" + oldId)));
            assertTrue(msgs.stream().anyMatch(msg -> msg.startsWith("Failed to process response")
                && msg.contains("connectionId=" + oldId) && msg.contains("requestId=" + failedStart.requestId())));
        }

        assertEquals(2, closed.size());
        assertEquals(connections.get(1).connectionId(), closed.get(1).connectionId());
        assertEquals(2, starts.size());

        for (ConnectionDescription start : starts)
            assertNull(start.connectionId());
    }

    /** Tests that a client not advertising the feature receives the original handshake response. */
    @Test
    public void testHandshakeWithoutConnectionIdFeature() throws Exception {
        try (Socket sock = new Socket(clientHost(grid(0).localNode()), clientPort(grid(0).localNode()))) {
            sock.setSoTimeout(10_000);

            try (BinaryWriterEx writer = BinaryUtils.writerWithoutSchema(null, BinaryStreams.outputStream(32))) {
                writer.writeInt(0);
                writer.writeByte((byte)HANDSHAKE);
                writer.writeShort(LATEST_VER.major());
                writer.writeShort(LATEST_VER.minor());
                writer.writeShort(LATEST_VER.patch());
                writer.writeByte(THIN_CLIENT);
                writer.writeByteArray(new byte[0]);
                writer.out().writeInt(0, writer.out().position() - 4);

                sock.getOutputStream().write(writer.out().arrayCopy());
            }

            DataInputStream in = new DataInputStream(sock.getInputStream());
            byte[] res = new byte[Integer.reverseBytes(in.readInt())];

            in.readFully(res);

            try (BinaryReaderEx reader = BinaryUtils.reader(null, BinaryStreams.inputStream(res), null, true, true)) {
                assertTrue(reader.readBoolean());
                assertArrayEquals(new byte[0], reader.readByteArray());
                assertEquals(grid(0).localNode().id(), reader.readUuid());
                assertEquals(0, reader.available());
            }
        }
    }

    /** @param conn Connection description to compare with the server system view. */
    private void assertConnectionId(ConnectionDescription conn) {
        assertNotNull(conn.connectionId());
        assertEquals(grid(0).localNode().id(), conn.serverNodeId());

        SystemView<ClientConnectionView> view = grid(0).context().systemView().view(CLI_CONN_VIEW);

        for (ClientConnectionView row : view) {
            if (row.remoteAddress().equals(conn.localAddress())) {
                assertEquals(Long.valueOf(row.connectionId()), conn.connectionId());

                return;
            }
        }

        fail("Connection is missing from SYS.CLIENT_CONNECTIONS: " + conn.connectionId());
    }
}
