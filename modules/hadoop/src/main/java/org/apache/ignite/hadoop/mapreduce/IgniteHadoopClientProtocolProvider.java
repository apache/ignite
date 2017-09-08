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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.processors.hadoop.impl.proto.HadoopClientProtocol;
import org.apache.ignite.internal.processors.hadoop.mapreduce.MapReduceClient;
import org.apache.ignite.internal.util.typedef.F;


/**
 * Ignite Hadoop client protocol provider.
 */
public class IgniteHadoopClientProtocolProvider extends ClientProtocolProvider {
    /** Framework name used in configuration. */
    public static final String FRAMEWORK_NAME = "ignite";

    /** Clients. */
    private final ConcurrentHashMap<String, MapReduceClient> cliMap = new ConcurrentHashMap<>();

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
    @Override public void close(ClientProtocol cliProto) throws IOException {
        if (cliProto instanceof HadoopClientProtocol) {
            MapReduceClient cli = ((HadoopClientProtocol)cliProto).client();

            if (cli.release())
                cliMap.remove(cli.cluster(), cli);
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
    private MapReduceClient client(String clusterName, Collection<String> addrs) throws IOException {
        while (true) {
            MapReduceClient cli = cliMap.get(clusterName);

            if (cli == null) {
                cli = new MapReduceClient(clusterName, addrs);

                MapReduceClient oldCli = cliMap.putIfAbsent(clusterName, cli);

                if (oldCli != null)
                    cli = oldCli;
            }

            if (cli.acquire())
                return cli;
            else
                cliMap.remove(clusterName, cli);
        }
    }
}