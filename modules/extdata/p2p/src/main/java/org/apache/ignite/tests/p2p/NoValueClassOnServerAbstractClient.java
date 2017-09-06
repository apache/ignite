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

package org.apache.ignite.tests.p2p;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public abstract class NoValueClassOnServerAbstractClient implements AutoCloseable {
    /** */
    protected final Ignite ignite;

    /** */
    private final IgniteLogger log;

    /**
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public NoValueClassOnServerAbstractClient(String[] args) throws Exception {
        System.out.println("Starting test client node.");

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setClientMode(true);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        ignite = Ignition.start(cfg);

        System.out.println("Test external node started");

        log = ignite.log().getLogger(getClass());

        log.info("Started node [id=" + ignite.cluster().localNode().id() +
            ", marsh=" + ignite.configuration().getMarshaller().getClass().getSimpleName() + ']');
    }

    /**
     * @param msg Message.
     */
    protected final void info(String msg) {
        log.info(msg);
    }


    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        ignite.close();
    }

    /**
     * @throws Exception If failed.
     */
    protected abstract void runTest() throws Exception;
}
