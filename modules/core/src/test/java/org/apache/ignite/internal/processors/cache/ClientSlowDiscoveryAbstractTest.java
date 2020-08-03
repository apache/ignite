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

package org.apache.ignite.internal.processors.cache;

import java.util.function.Supplier;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ClientSlowDiscoveryAbstractTest extends GridCommonAbstractTest {
    /** Cache name. */
    protected static final String CACHE_NAME = "cache";

    /** Cache configuration. */
    private final CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME)
        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        .setReadFromBackup(false)
        .setBackups(1)
        .setAffinity(new RendezvousAffinityFunction(false, 64));

    /** Communication SPI supplier. */
    protected Supplier<CommunicationSpi> communicationSpiSupplier = TestRecordingCommunicationSpi::new;

    /** Discovery SPI supplier. */
    protected Supplier<DiscoverySpi> discoverySpiSupplier = TcpDiscoverySpi::new;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);
        cfg.setCacheConfiguration(ccfg);
        cfg.setCommunicationSpi(communicationSpiSupplier.get());
        cfg.setDiscoverySpi(discoverySpiSupplier.get());

        return cfg;
    }

    /**
     *
     */
    static class NodeJoinInterceptingDiscoverySpi extends TcpDiscoverySpi {
        /** Interceptor. */
        protected volatile IgniteInClosure<TcpDiscoveryNodeAddFinishedMessage> interceptor;

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage && interceptor != null)
                interceptor.apply((TcpDiscoveryNodeAddFinishedMessage) msg);
        }
    }

    /**
     *
     */
    static class CustomMessageInterceptingDiscoverySpi extends TcpDiscoverySpi {
        /** Interceptor. */
        protected volatile IgniteInClosure<DiscoveryCustomMessage> interceptor;

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (!(msg instanceof TcpDiscoveryCustomEventMessage))
                return;

            TcpDiscoveryCustomEventMessage cm = (TcpDiscoveryCustomEventMessage)msg;

            DiscoveryCustomMessage delegate;

            try {
                DiscoverySpiCustomMessage custMsg = cm.message(marshaller(),
                    U.resolveClassLoader(ignite().configuration()));

                assertNotNull(custMsg);

                delegate = ((CustomMessageWrapper)custMsg).delegate();
            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            if (interceptor != null)
                interceptor.apply(delegate);
        }
    }
}
