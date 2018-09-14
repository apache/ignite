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

package org.apache.ignite.internal.client.thin;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.HostAndPortRange;

/**
 * Adds failover abd thread-safety to {@link ClientChannel}.
 */
final class ReliableChannel implements AutoCloseable {
    /** Raw channel. */
    private final Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory;

    /** Service lock. */
    private final Lock svcLock = new ReentrantLock();

    /** Primary server. */
    private InetSocketAddress primary;

    /** Backup servers. */
    private final Deque<InetSocketAddress> backups = new LinkedList<>();

    /** Channel. */
    private ClientChannel ch = null;

    /** Ignite config. */
    private final ClientConfiguration clientCfg;

    /**
     * Constructor.
     */
    ReliableChannel(
        Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory,
        ClientConfiguration clientCfg
    ) throws ClientException {
        if (chFactory == null)
            throw new NullPointerException("chFactory");

        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        this.chFactory = chFactory;
        this.clientCfg = clientCfg;

        List<InetSocketAddress> addrs = parseAddresses(clientCfg.getAddresses());

        primary = addrs.get(new Random().nextInt(addrs.size())); // we already verified there is at least one address

        ch = chFactory.apply(new ClientChannelConfiguration(clientCfg).setAddress(primary)).get();

        for (InetSocketAddress a : addrs)
            if (a != primary)
                this.backups.add(a);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (ch != null) {
            ch.close();

            ch = null;
        }
    }

    /**
     * Send request and handle response. The method is synchronous and single-threaded.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<BinaryOutputStream> payloadWriter,
        Function<BinaryInputStream, T> payloadReader
    ) throws ClientException {
        ClientConnectionException failure = null;

        T res = null;

        int totalSrvs = 1 + backups.size();

        svcLock.lock();
        try {
            for (int i = 0; i < totalSrvs; i++) {
                try {
                    if (failure != null)
                        changeServer();

                    if (ch == null)
                        ch = chFactory.apply(new ClientChannelConfiguration(clientCfg).setAddress(primary)).get();

                    long id = ch.send(op, payloadWriter);

                    res = ch.receive(op, id, payloadReader);

                    failure = null;

                    break;
                }
                catch (ClientConnectionException e) {
                    if (failure == null)
                        failure = e;
                    else
                        failure.addSuppressed(e);
                }
            }
        }
        finally {
            svcLock.unlock();
        }

        if (failure != null)
            throw failure;

        return res;
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOperation op, Function<BinaryInputStream, T> payloadReader)
        throws ClientException {
        return service(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<BinaryOutputStream> payloadWriter) throws ClientException {
        service(op, payloadWriter, null);
    }

    /**
     * @return Server version.
     */
    public ProtocolVersion serverVersion() {
        return ch.serverVersion();
    }

    /**
     * @return host:port_range address lines parsed as {@link InetSocketAddress}.
     */
    private static List<InetSocketAddress> parseAddresses(String[] addrs) throws ClientException {
        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            try {
                ranges.add(HostAndPortRange.parse(
                    a,
                    ClientConnectorConfiguration.DFLT_PORT,
                    ClientConnectorConfiguration.DFLT_PORT + ClientConnectorConfiguration.DFLT_PORT_RANGE,
                    "Failed to parse Ignite server address"
                ));
            }
            catch (IgniteCheckedException e) {
                throw new ClientException(e);
            }
        }

        return ranges.stream()
            .flatMap(r -> IntStream
                .rangeClosed(r.portFrom(), r.portTo()).boxed()
                .map(p -> new InetSocketAddress(r.host(), p))
            )
            .collect(Collectors.toList());
    }

    /** */
    private void changeServer() {
        if (backups.size() > 0) {
            backups.addLast(primary);

            primary = backups.removeFirst();

            try {
                ch.close();
            }
            catch (Exception ignored) {
            }

            ch = null;
        }
    }
}
