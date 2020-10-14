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

package org.apache.ignite.client;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.util.IgniteStopwatch;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Checks if it can connect to a valid address from the node address list.
 */
public class ConnectionTest {
    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testEmptyNodeAddress() throws Exception {
        testConnection("");
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testNullNodeAddress() throws Exception {
        testConnection(null);
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testNullNodeAddresses() throws Exception {
        testConnection(null, null);
    }

    /** */
    @Test
    public void testValidNodeAddresses() throws Exception {
        testConnection(Config.SERVER);
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientConnectionException.class)
    public void testInvalidNodeAddresses() throws Exception {
        testConnection("127.0.0.1:47500", "127.0.0.1:10801");
    }

    /** */
    @Test
    public void testValidInvalidNodeAddressesMix() throws Exception {
        testConnection("127.0.0.1:47500", "127.0.0.1:10801", Config.SERVER);
    }

    /** */
    @Test
    public void testAsynchronousSocketChannel() throws Exception {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1)) {
            IgniteStopwatch sw = IgniteStopwatch.createStarted();

            for (int i = 0; i < 10000; i++) {
                handshakeOld().get();
            }

            System.out.println(">>> " + sw.elapsed().toMillis());
        }
    }

    /** 28800ms */
    private CompletableFuture<Integer> handshakeOld() throws Exception {
        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("localhost:10800"));
        client.close();

        return CompletableFuture.completedFuture(12);
    }

    /** 5600ms */
    private CompletableFuture<Integer> handshakeAsyncChannel() throws IOException {
        CompletableFuture<Integer> fut = new CompletableFuture<>();

        // Connect.
        AsynchronousSocketChannel client = AsynchronousSocketChannel.open();
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", 10800);
        client.connect(hostAddress, null, new CompletionHandler<Void, Object>() {
            @Override
            public void completed(Void unused, Object o) {
                // Handshake.
                BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);
                try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, new BinaryHeapOutputStream(32), null, null)) {
                    writer.writeInt(12); // reserve an integer for the request size

                    writer.writeByte((byte) ClientListenerRequest.HANDSHAKE);
                    writer.writeShort(1);
                    writer.writeShort(0);
                    writer.writeShort(0);
                    writer.writeByte(ClientListenerNioListener.THIN_CLIENT);

                    client.write(ByteBuffer.wrap(writer.array()), null, new CompletionHandler<Integer, Object>() {
                        @Override
                        public void completed(Integer integer, Object o) {
                            fut.complete(integer);
                        }

                        @Override
                        public void failed(Throwable throwable, Object o) {
                            fut.completeExceptionally(throwable);
                        }
                    });
                }
                catch (IOException e) {
                    fut.completeExceptionally(e);
                }
            }

            @Override
            public void failed(Throwable throwable, Object o) {
                fut.completeExceptionally(throwable);
            }
        });

        fut.thenAccept(i -> {
            try {
                client.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });

        return fut;
    }

    /**
     * @param addrs Addresses to connect.
     */
    private void testConnection(String... addrs) throws Exception {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(new ClientConfiguration()
                     .setAddresses(addrs))) {
        }
    }
}
