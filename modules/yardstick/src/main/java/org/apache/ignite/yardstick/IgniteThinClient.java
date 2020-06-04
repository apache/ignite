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

package org.apache.ignite.yardstick;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.thin.cache.IgniteThinBenchmarkUtils;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Thin client.
 */
@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
public class IgniteThinClient implements AutoCloseable {
    /** Thin client pool. */
    private final ClientPool clientPool;

    /** */
    public IgniteThinClient(BenchmarkConfiguration cfg) {
        IgniteThinBenchmarkArguments args = new IgniteThinBenchmarkArguments();

        BenchmarkUtils.jcommander(cfg.commandLineArguments(), args, "<ignite-node>");

        ClientConfiguration clCfg = new ClientConfiguration();

        String[] hosts = IgniteThinBenchmarkUtils.servHostArr(cfg);
        List<String> addrs = new ArrayList<>(hosts.length);

        Arrays.sort(hosts);

        String prevHost = null;
        int prevPort = 0;

        for (String host : hosts) {
            int port = host.equals(prevHost) ? prevPort + 1 : ClientConnectorConfiguration.DFLT_PORT;

            addrs.add(host + ':' + port);

            prevHost = host;
            prevPort = port;
        }

        BenchmarkUtils.println("Client parameters: " + args);
        BenchmarkUtils.println("Using for connection addresses: " + addrs);

        clCfg.setAddresses(addrs.toArray(new String[addrs.size()]));
        clCfg.setPartitionAwarenessEnabled(args.clientPartitionsAware());

        ClientPool clientPool;

        switch (args.clientPoolType()) {
            case THREAD_LOCAL:
                clientPool = new ThreadLocalClientPool(args.connectionTimeout(), clCfg);
                break;

            case SINGLE_CLIENT:
                clientPool = new SingleClientPool(args.connectionTimeout(), clCfg);
                break;

            case ROUND_ROBIN:
                clientPool = new RoundRobinClientPool(args.connectionTimeout(), clCfg, args.clientPoolSize());
                break;

            default:
                throw new IllegalArgumentException("Unexpected client pool type");
        }

        this.clientPool = clientPool;
    }

    /**
     * @return Thin client.
     */
    public IgniteClient get() {
        return clientPool.get();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        clientPool.close();
    }

    /** Client pool */
    private abstract static class ClientPool implements AutoCloseable {
        /** Created time. */
        private final long created = System.nanoTime();

        /** Timeout to wait for servers startup. */
        private final long timeout;

        /** Clients. */
        protected final List<IgniteClient> clients = new CopyOnWriteArrayList<>();

        /** Client configuration. */
        protected final ClientConfiguration cfg;

        /** */
        protected ClientPool(long timeout, ClientConfiguration cfg) {
            this.timeout = timeout;
            this.cfg = cfg;
        }

        /** */
        protected IgniteClient startClient() {
            ClientConnectionException err;

            // Sometimes server-side client connector is not ready right after driver startup,
            // try to reconnect until connection timeout is reached.
            do {
                try {
                    IgniteClient client = Ignition.startClient(cfg);

                    clients.add(client);

                    return client;
                } catch (ClientConnectionException e) {
                    err = e;
                }
            } while (U.millisSinceNanos(created) < timeout);

            throw err;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            for (IgniteClient client : clients)
                client.close();
        }

        /** */
        public abstract IgniteClient get();
    }

    /** */
    private static class SingleClientPool extends ClientPool {
        /** */
        public SingleClientPool(long timeout, ClientConfiguration cfg) {
            super(timeout, cfg);

            startClient();
        }

        /** {@inheritDoc} */
        @Override public IgniteClient get() {
            return clients.get(0);
        }
    }

    /** */
    private static class ThreadLocalClientPool extends ClientPool {
        /** Client thread local. */
        private final ThreadLocal<IgniteClient> clientThreadLoc;

        /** */
        public ThreadLocalClientPool(long timeout, ClientConfiguration cfg) {
            super(timeout, cfg);

            clientThreadLoc = ThreadLocal.withInitial(this::startClient);
        }

        /** {@inheritDoc} */
        @Override public IgniteClient get() {
            return clientThreadLoc.get();
        }
    }

    /** */
    private static class RoundRobinClientPool extends ClientPool {
        /** Current client index. */
        private final AtomicInteger curIdx = new AtomicInteger();

        /** */
        public RoundRobinClientPool(long timeout, ClientConfiguration cfg, int totalCnt) {
            super(timeout, cfg);

            for (int i = 0; i < totalCnt; i++)
                startClient();
        }

        /** {@inheritDoc} */
        @Override public IgniteClient get() {
            return clients.get((curIdx.incrementAndGet() & Integer.MAX_VALUE) % clients.size());
        }
    }
}
