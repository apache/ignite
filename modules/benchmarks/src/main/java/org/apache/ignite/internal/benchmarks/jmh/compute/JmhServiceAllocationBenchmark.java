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

package org.apache.ignite.internal.benchmarks.jmh.compute;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhNodeStopHook;
import org.apache.ignite.services.Service;
import org.openjdk.jmh.annotations.Benchmark;

/** Calls of a service deployed on each of the two servers, through a client node's proxy and a thin client's proxy. */
public class JmhServiceAllocationBenchmark extends JmhCacheAbstractBenchmark {
    /** Service name. */
    private static final String SVC = "echo";

    /** Proxy of the client node. */
    private EchoService proxy;

    /** Thin client, connected to the servers. */
    private IgniteClient thin;

    /** Proxy of the thin client. */
    private EchoService thinProxy;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        node.services(node.cluster().forServers()).deployNodeSingleton(SVC, new EchoServiceImpl());

        proxy = node.services().serviceProxy(SVC, EchoService.class, false);

        thin = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800", "127.0.0.1:10801"));
        thinProxy = thin.services().serviceProxy(SVC, EchoService.class);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        // Profilers read the last iteration while the nodes still run.
        JmhNodeStopHook.run();

        thin.close();

        Ignition.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected int dataNodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientMode() {
        return true;
    }

    /** @return The argument, returned by the service through the client node's proxy. */
    @Benchmark
    public int call() {
        return proxy.echo(1);
    }

    /** @return The argument, returned by the service through the thin client's proxy. */
    @Benchmark
    public int thinCall() {
        return thinProxy.echo(1);
    }

    /** Service interface. */
    public interface EchoService {
        /**
         * @param x Argument.
         * @return The argument.
         */
        int echo(int x);
    }

    /** Service that returns its argument. */
    public static class EchoServiceImpl implements EchoService, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int echo(int x) {
            return x;
        }
    }
}
