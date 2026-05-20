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

package org.apache.ignite.internal.managers.communication;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.plugin.AbstractMarshallableMessageFactoryProvider;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MessageFactoryMarshallerInitializationTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicInteger initCnt = new AtomicInteger();

    /** */
    public MessageFactoryMarshallerInitializationTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initCnt.set(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testInitFromPlugin() throws Exception {
        checkInits(
            getConfiguration().setPluginProviders(new PluginWithMessageFactory()),
            1);
    }

    /** */
    @Test
    public void testInitFromDiscovery() throws Exception {
        checkInits(
            getConfiguration().setDiscoverySpi(new DiscoverySpiWithExtraFactory()),
            1);
    }

    /** */
    @Test
    public void testInitFromPluginAndDiscovery() throws Exception {
        checkInits(
            getConfiguration()
                .setPluginProviders(new PluginWithMessageFactory())
                .setDiscoverySpi(new DiscoverySpiWithExtraFactory()),
            2);
    }

    /**
     * @param cfg Configuration.
     * @param exp Expected count.
     */
    private void checkInits(IgniteConfiguration cfg, int exp) throws Exception {
        assertEquals(0, initCnt.get());

        startGrid(cfg);

        assertEquals(exp, initCnt.get());
    }

    /** Message factory provider, which counts initializations. */
    private static class TestMessageFactoryProvider extends AbstractMarshallableMessageFactoryProvider {
        /** {@inheritDoc} */
        @Override public void registerAll(MessageFactory factory) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(Marshaller dfltMarsh, Marshaller schemaAwareMarsh, ClassLoader resolvedClsLdr) {
            super.init(dfltMarsh, schemaAwareMarsh, resolvedClsLdr);

            initCnt.incrementAndGet();
        }
    }

    /** */
    private static class PluginWithMessageFactory extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            super.initExtensions(ctx, registry);

            registry.registerExtension(MessageFactoryProvider.class, new TestMessageFactoryProvider());
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "PluginWithMessageFactory";
        }

        /** {@inheritDoc} */
        @Override public void validateNewNode(ClusterNode node, Serializable data) {
            super.validateNewNode(node, data);
        }
    }

    /** */
    private static class DiscoverySpiWithExtraFactory extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override public MessageFactoryProvider messageFactoryProvider() {
            return new TestMessageFactoryProvider();
        }
    }
}
