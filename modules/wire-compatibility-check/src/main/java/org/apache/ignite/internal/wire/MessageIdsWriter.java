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

package org.apache.ignite.internal.wire;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.MessageProcessor;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory;

/** Writes the direct types of the registered messages with their classes to {@code ids.json}. */
public class MessageIdsWriter {
    /** */
    private final IgniteMessageFactoryImpl<?, ?> factory;

    /** @param providers Message factory providers. */
    public MessageIdsWriter(MessageFactoryProvider[] providers) {
        factory = new IgniteMessageFactoryImpl<>(providers);
    }

    /** @param args Directory with the message descriptions. */
    public static void main(String[] args) throws IOException, IgniteCheckedException {
        List<MessageFactoryProvider> providers = new ArrayList<>();

        providers.add(new CoreMessagesProvider());

        for (IgniteComponentType type : IgniteComponentType.values()) {
            MessageFactoryProvider provider = type.messageFactory();

            if (provider != null)
                providers.add(provider);
        }

        providers.add(new ZkMessageFactory());

        new MessageIdsWriter(providers.toArray(MessageFactoryProvider[]::new)).write(Path.of(args[0]));
    }

    /**
     * Writes {@code ids.json} to the directory with the message descriptions.
     *
     * @param dir Directory with the message descriptions.
     * @throws IOException If the file cannot be written.
     * @throws IllegalStateException If a registered message has no description.
     */
    public void write(Path dir) throws IOException {
        List<String> lines = new ArrayList<>();

        for (short id : factory.registeredDirectTypes()) {
            String clsName = factory.create(id).getClass().getName();

            if (!MessageProcessor.SKIP_MESSAGES.contains(clsName) && !Files.exists(dir.resolve("messages/" + clsName + ".json")))
                throw new IllegalStateException("No wire format description [cls=" + clsName + ']');

            lines.add("  \"" + id + "\": \"" + clsName + '"');
        }

        Files.writeString(dir.resolve("ids.json"), "{\n" + String.join(",\n", lines) + "\n}\n");
    }
}
