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

package org.apache.ignite.internal.util.distributed;

import org.apache.ignite.internal.codegen.TestIntegerMessageSerializer;
import org.apache.ignite.internal.codegen.TestUuidMessageSerializer;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

import static org.junit.Assert.assertTrue;

/** */
public class MessagesPluginProvider extends AbstractTestPluginProvider {
    /** */
    private static final MessageFactoryProvider FACTORY_PROVIDER = f -> {
        f.register((short)10_000, TestIntegerMessage::new, new TestIntegerMessageSerializer());
        f.register((short)10_001, TestUuidMessage::new, new TestUuidMessageSerializer());
    };

    /** {@inheritDoc} */
    @Override public String name() {
        return "Distributed process test messages plugin";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        registry.registerExtension(MessageFactoryProvider.class, FACTORY_PROVIDER);

        // Register messages into the DiscoverySpi.
        assertTrue(ctx.igniteConfiguration().getDiscoverySpi() instanceof MessagesInjectedTcpDiscoverySpi);
    }

    /** */
    public static class MessagesInjectedTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void initLocalNode(int srvPort, boolean addExtAddrAttr) {
            GridTestUtils.setFieldValue(this, TcpDiscoverySpi.class, "msgFactory", new IgniteMessageFactoryImpl(
                new MessageFactoryProvider[] { new DiscoveryMessageFactory(), FACTORY_PROVIDER}));

            super.initLocalNode(srvPort, addExtAddrAttr);
        }
    }
}
