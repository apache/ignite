/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tools.compatibility.messages;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory;

/** Exports the actual production registrations and their compiled field descriptions. */
public class MessageTable {
    /** No instances. */
    private MessageTable() {
        // No-op.
    }

    /**
     * Exports the registered messages to an XML file.
     *
     * @param args Output file path.
     * @throws Exception If the build is incomplete or the inputs cannot be read.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1)
            throw new IllegalArgumentException("Expected: output-file");

        Path out = Path.of(args[0]).toAbsolutePath();
        String table = new XmlTableWriter().write(collect());

        Files.createDirectories(out.getParent());
        Files.writeString(out, table);
    }

    /**
     * @return Collected message table.
     * @throws Exception If any registered message cannot be described.
     */
    static Data collect() throws IOException {
        MessageFactoryProvider[] providers = {
            new CoreMessagesProvider(),
            new GridH2ValueMessageFactory(),
            new CalciteMessageFactory(),
            new ZkMessageFactory()
        };

        IgniteMessageFactoryImpl<?, ?> factory = new IgniteMessageFactoryImpl<>(providers);
        short[] ids = factory.registeredDirectTypes();

        Arrays.sort(ids);

        List<MessageRepresentation> msgs = new ArrayList<>();

        try (MessageSchema schemas = new MessageSchema()) {
            for (short id : ids) {
                Message msg = factory.create(id);

                Class<?> cls = msg.getClass();

                msgs.add(new MessageRepresentation(id, cls.getName(), schemas.read(cls)));
            }
        }

        List<String> providerNames = Arrays.stream(providers).map(p -> p.getClass().getName()).toList();

        return new Data(providerNames, msgs);
    }

    /**
     * Collected input for XML serialization.
     *
     * @param providers Provider class names.
     * @param messages Messages sorted by registered ID.
     */
    record Data(List<String> providers, List<MessageRepresentation> messages) {
        // No-op.
    }

}
