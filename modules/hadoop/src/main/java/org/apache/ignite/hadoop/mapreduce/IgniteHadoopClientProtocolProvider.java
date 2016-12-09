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

package org.apache.ignite.hadoop.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.marshaller.jdk.GridClientJdkMarshaller;
import org.apache.ignite.internal.processors.hadoop.impl.proto.HadoopClientProtocol;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.client.GridClientProtocol.TCP;


/**
 * Ignite Hadoop client protocol provider.
 */
public class IgniteHadoopClientProtocolProvider extends ClientProtocolProvider {
    /** Framework name used in configuration. */
    public static final String FRAMEWORK_NAME = "ignite";

    /** Clients. */
    private final ConcurrentHashMap<String, IgniteInternalFuture<ClientData>> cliMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public ClientProtocol create(Configuration conf) throws IOException {
        if (FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME))) {
            Collection<String> addrs = conf.getTrimmedStringCollection(MRConfig.MASTER_ADDRESS);

            if (F.isEmpty(addrs))
                throw new IOException("Failed to create client protocol because Ignite node addresses are not " +
                    "specified (did you set " + MRConfig.MASTER_ADDRESS + " property?).");

            if (F.contains(addrs, "local"))
                throw new IOException("Local execution mode is not supported, please point " +
                    MRConfig.MASTER_ADDRESS + " to real Ignite nodes.");

            Collection<String> addrs0 = new ArrayList<>(addrs.size());

            // Set up port by default if need
            for (String addr : addrs) {
                if (!addr.contains(":"))
                    addrs0.add(addr + ':' + ConnectorConfiguration.DFLT_TCP_PORT);
                else
                    addrs0.add(addr);
            }

            return new HadoopClientProtocol(conf, client(conf.get(MRConfig.MASTER_ADDRESS), addrs0));
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ClientProtocol create(InetSocketAddress addr, Configuration conf) throws IOException {
        if (FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME)))
            return createProtocol(addr.getHostString() + ":" + addr.getPort(), conf);

        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void close(ClientProtocol cliProto) throws IOException {
        if (cliProto instanceof HadoopClientProtocol) {
            ClientData clientData = ((HadoopClientProtocol)cliProto).getClientData();

            if (clientData.decrementUsages())
                // Remove the client from the cache:
                cliMap.remove(clientData.getKey(), clientData.getFut());

        }
    }

    /**
     * Internal protocol creation routine.
     *
     * @param addr Address.
     * @param conf Configuration.
     * @return Client protocol.
     * @throws IOException If failed.
     */
    private ClientProtocol createProtocol(String addr, Configuration conf) throws IOException {
        return new HadoopClientProtocol(conf, client(addr, Collections.singletonList(addr)));
    }

    /**
     * Create client.
     *
     * @param clusterName Ignite cluster logical name.
     * @param addrs Endpoint addresses.
     * @return Client.
     * @throws IOException If failed.
     */
    @SuppressWarnings("unchecked")
    private ClientData client0(final String clusterName,
        final Collection<String> addrs) throws IOException {
        try {
            IgniteInternalFuture<ClientData> fut = cliMap.get(clusterName);

            if (fut == null) {
                GridFutureAdapter<ClientData> fut0 = new GridFutureAdapter<>();

                IgniteInternalFuture<ClientData> oldFut = cliMap.putIfAbsent(clusterName, fut0);

                if (oldFut != null)
                    return oldFut.get();
                else {
                    GridClientConfiguration cliCfg = new GridClientConfiguration();

                    cliCfg.setProtocol(TCP);
                    cliCfg.setServers(addrs);
                    cliCfg.setMarshaller(new GridClientJdkMarshaller());
                    cliCfg.setMaxConnectionIdleTime(24 * 60 * 60 * 1000L); // 1 day.
                    cliCfg.setDaemon(true);

                    try {
                        GridClient cli = GridClientFactory.start(cliCfg);

                        ClientData cd = new ClientData(cli, clusterName, fut0);

                        fut0.onDone(cd);

                        return cd;
                    }
                    catch (GridClientException e) {
                        fut0.onDone(e);

                        throw new IOException("Failed to establish connection with Ignite: " + addrs, e);
                    }
                }
            }
            else
                return fut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to establish connection with Ignite node: " + addrs, e);
        }
    }

    /**
     * Gets the client with possible retries.
     *
     * @param clusterName The cluster name.
     * @param addrs The list of addresses.
     * @return The client.
     * @throws IOException On error.
     */
    private ClientData client(final String clusterName, final Collection<String> addrs) throws IOException {
        while (true) {
            ClientData cd = client0(clusterName, addrs);

            int usages = cd.incrementUsages();

            assert usages != 0;

            // If usages count is negative, this means that the client is dead and cannot be used any more.
            // So, we should continue the loop and get a new one:
            if (usages > 0)
                return cd;
        }
    }

    /**
     * The client data structure.
     */
    public static class ClientData {
        /** The grid client. */
        private final GridClient gridClient;
        /** The key. */
        private final String key;
        /** The future. */
        private final GridFutureAdapter<ClientData> fut;
        /** The usage counter. */
        private final AtomicInteger usageCnt = new AtomicInteger();

        /**
         * Constructor.
         *
         * @param gridClient The client.
         * @param key The client.
         * @param fut The future.
         */
        ClientData(GridClient gridClient, String key, GridFutureAdapter<ClientData> fut) {
            this.gridClient = gridClient;
            this.key = key;
            this.fut = fut;
        }

        /**
         * Gets the client.
         *
         * @return The client.
         */
        public GridClient getGridClient() {
            return gridClient;
        }

        /**
         * Gets the key.
         *
         * @return the key.
         */
        String getKey() {
            return key;
        }

        /**
         * Gets the future.
         *
         * @return The future.
         */
        GridFutureAdapter<ClientData> getFut() {
            return fut;
        }

        /**
         * Increments usage count.
         *
         * @return The usage count after increment.
         */
        public int incrementUsages() {
            while (true) {
                int cur = usageCnt.get();

                if (cur < 0)
                    return cur; // Negative result, client is dead;

                int next = cur + 1;

                if (usageCnt.compareAndSet(cur, next))
                    return next;
            }
        }

        /**
         * Decrements the usages of the client and closes it if this is the last usage.
         *
         * @return Iff the client was closed as a result of this call.
         */
        boolean decrementUsages() {
            while (true) {
                int cur = usageCnt.get();

                if (cur < 0)
                    return false; // Already closed, nothing to do.

                // If there is no or only one usage, set -1 to indicate
                // that the client is closed.
                int next = cur <= 1 ? -1 : cur - 1;

                if (usageCnt.compareAndSet(cur, next)) {
                    if (next < 0) {
                        // We should close the client:
                        close0();

                        return true; // Closed.
                    }

                    return false;
                }
            }
        }

        /**
         * Client close implementation.
         */
        private void close0() {
            getGridClient().close();
        }
    }
}