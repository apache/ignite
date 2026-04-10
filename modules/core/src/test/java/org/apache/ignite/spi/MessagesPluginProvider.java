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

package org.apache.ignite.spi;

import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;

/**
 * Plugin provider for registering test messages in the communication and discovery protocols.
 */
public class MessagesPluginProvider extends AbstractTestPluginProvider {
    /** */
    private final MessageFactoryProvider msgFactoryProvider;

    /** */
    @SafeVarargs
    public MessagesPluginProvider(Class<? extends Message>... msgs) {
        msgFactoryProvider = f -> {
            short directType = CoreMessagesProvider.MAX_MESSAGE_ID + 1;

            for (Class<? extends Message> msg : msgs) {
                Supplier<Message> msgSupp = () -> {
                    try {
                        return U.newInstance(msg);
                    }
                    catch (IgniteCheckedException e) {
                        throw new RuntimeException(e);
                    }
                };

                f.register(directType, msgSupp, loadSerializer(msg));

                directType++;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "Test messages plugin";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        // Register messages into the communication protocol.
        registry.registerExtension(MessageFactoryProvider.class, msgFactoryProvider);
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        // Register messages into the discovery protocol.
        TestTcpDiscoverySpi discoSpi = (TestTcpDiscoverySpi)ctx.igniteConfiguration().getDiscoverySpi();

        discoSpi.messageFactory(msgFactoryProvider);
    }

    /** */
    private MessageSerializer<? extends Message> loadSerializer(Class<? extends Message> msgCls) {
        try {
            Class<?> serCls = U.gridClassLoader()
                .loadClass(msgCls.getPackage().getName() + "." + msgCls.getSimpleName() + "Serializer");

            return (MessageSerializer<? extends Message>)U.newInstance(serCls);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to find serializer for message: " + msgCls, e);
        }
    }
}
