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
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.marshaller.jdk.GridClientJdkMarshaller;
import org.apache.ignite.internal.processors.hadoop.proto.HadoopClientProtocol;
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
    private static final ConcurrentHashMap<String, IgniteInternalFuture<GridClient>> cliMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public ClientProtocol create(Configuration conf) throws IOException {
        if (FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME))) {
            String addr = conf.get(MRConfig.MASTER_ADDRESS);

            if (F.isEmpty(addr))
                throw new IOException("Failed to create client protocol because server address is not specified (is " +
                    MRConfig.MASTER_ADDRESS + " property set?).");

            if (F.eq(addr, "local"))
                throw new IOException("Local execution mode is not supported, please point " +
                    MRConfig.MASTER_ADDRESS + " to real Ignite node.");

            return createProtocol(addr, conf);
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
    @Override public void close(ClientProtocol cliProto) throws IOException {
        // No-op.
    }

    /**
     * Internal protocol creation routine.
     *
     * @param addr Address.
     * @param conf Configuration.
     * @return Client protocol.
     * @throws IOException If failed.
     */
    private static ClientProtocol createProtocol(String addr, Configuration conf) throws IOException {
        return new HadoopClientProtocol(conf, client(addr));
    }

    /**
     * Create client.
     *
     * @param addr Endpoint address.
     * @return Client.
     * @throws IOException If failed.
     */
    private static GridClient client(String addr) throws IOException {
        try {
            IgniteInternalFuture<GridClient> fut = cliMap.get(addr);

            if (fut == null) {
                GridFutureAdapter<GridClient> fut0 = new GridFutureAdapter<>();

                IgniteInternalFuture<GridClient> oldFut = cliMap.putIfAbsent(addr, fut0);

                if (oldFut != null)
                    return oldFut.get();
                else {
                    GridClientConfiguration cliCfg = new GridClientConfiguration();

                    cliCfg.setProtocol(TCP);
                    cliCfg.setServers(Collections.singletonList(addr));
                    cliCfg.setMarshaller(new GridClientJdkMarshaller());
                    cliCfg.setMaxConnectionIdleTime(24 * 60 * 60 * 1000L); // 1 day.
                    cliCfg.setDaemon(true);

                    try {
                        GridClient cli = GridClientFactory.start(cliCfg);

                        fut0.onDone(cli);

                        return cli;
                    }
                    catch (GridClientException e) {
                        fut0.onDone(e);

                        throw new IOException("Failed to establish connection with Ignite node: " + addr, e);
                    }
                }
            }
            else
                return fut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to establish connection with Ignite node: " + addr, e);
        }
    }
}