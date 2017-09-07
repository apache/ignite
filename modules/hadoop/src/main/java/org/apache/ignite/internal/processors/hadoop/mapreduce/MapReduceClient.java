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

package org.apache.ignite.internal.processors.hadoop.mapreduce;

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.marshaller.jdk.GridClientJdkMarshaller;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.internal.client.GridClientProtocol.TCP;

/**
 * Client.
 */
public class MapReduceClient {
    /** Cluster name. */
    private final String cluster;

    /** Addresses. */
    private final Collection<String> addrs;

    /** Mutex. */
    private final Object mux = new Object();

    /** Usage counter. */
    private final AtomicInteger cnt = new AtomicInteger();

    /** Client. */
    private volatile GridClient cli;

    /**
     * Constructor.
     *
     * @param cluster Cluster name.
     * @param addrs Addresses.
     */
    public MapReduceClient(String cluster, Collection<String> addrs) {
        this.cluster = cluster;
        this.addrs = addrs;
    }

    /**
     * @return Cluster name..
     */
    public String cluster() {
        return cluster;
    }

    /**
     * Gets the client.
     *
     * @return The client.
     */
    public GridClient client() throws IOException {
        GridClient cli0 = cli;

        if (cli0 == null) {
            synchronized (mux) {
                cli0 = cli;

                if (cli0 == null) {
                    GridClientConfiguration cliCfg = new GridClientConfiguration();

                    cliCfg.setProtocol(TCP);
                    cliCfg.setServers(addrs);
                    cliCfg.setMarshaller(new GridClientJdkMarshaller());
                    cliCfg.setMaxConnectionIdleTime(24 * 60 * 60 * 1000L); // 1 day.
                    cliCfg.setDaemon(true);

                    try {
                        cli0 = GridClientFactory.start(cliCfg);

                        cli = cli0;
                    }
                    catch (GridClientException e) {
                        throw new IOException("Failed to establish connection with Ignite: " + addrs, e);
                    }
                }
            }
        }

        return cli0;
    }

    /**
     * Increments usage count.
     *
     * @return {@code True} if succeeded and client can be used.
     */
    public boolean acquire() {
        while (true) {
            int cur = cnt.get();

            if (cur < 0)
                return false;

            int next = cur + 1;

            if (cnt.compareAndSet(cur, next))
                return true;
        }
    }

    /**
     * Decrements the usages of the client and closes it if this is the last usage.
     *
     * @return {@code True} if client can be closed safely by the called.
     */
    public boolean release() {
        int cnt0 = cnt.decrementAndGet();

        assert cnt0 >= 0;

        if (cnt0 == 0) {
            if (cnt.compareAndSet(0, -1)) {
                GridClient cli0 = cli;

                if (cli0 != null)
                    cli0.close();

                return true;
            }
        }

        return false;
    }
}
