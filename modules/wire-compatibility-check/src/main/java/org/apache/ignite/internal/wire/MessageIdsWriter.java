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
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.managers.communication.CompressedMessage;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory;

/** Writes the direct types of the registered messages with their classes to a JSON file. */
public class MessageIdsWriter {
    /** Directory of the message descriptions on the classpath. */
    private static final String WIRE_DIR = "META-INF/ignite-wire/messages/";

    /** */
    private final IgniteMessageFactoryImpl<?, ?> factory;

    /** Messages with custom {@code writeTo} and {@code readFrom}, which have no description. */
    private final Set<Class<?>> handWritten;

    /**
     * @param providers Message factory providers.
     * @param handWritten Messages with custom {@code writeTo} and {@code readFrom}, which have no description.
     */
    public MessageIdsWriter(MessageFactoryProvider[] providers, Set<Class<?>> handWritten) {
        factory = new IgniteMessageFactoryImpl<>(providers);
        this.handWritten = handWritten;
    }

    /** @param args Output file. */
    public static void main(String[] args) throws IOException, IgniteCheckedException {
        List<MessageFactoryProvider> providers = new ArrayList<>();

        providers.add(new CoreMessagesProvider());

        for (IgniteComponentType type : IgniteComponentType.values()) {
            MessageFactoryProvider provider = type.messageFactory();

            if (provider != null)
                providers.add(provider);
        }

        providers.add(new ZkMessageFactory());

        Set<Class<?>> handWritten = Set.of(CompressedMessage.class);

        new MessageIdsWriter(providers.toArray(MessageFactoryProvider[]::new), handWritten).write(Path.of(args[0]));
    }

    /**
     * Writes the direct types with the message classes to a file.
     *
     * @param file Output file.
     * @throws IOException If the file cannot be written.
     * @throws IllegalStateException If a registered message has no description.
     */
    public void write(Path file) throws IOException {
        List<String> lines = new ArrayList<>();

        for (short id : factory.registeredDirectTypes()) {
            Class<?> cls = factory.create(id).getClass();

            if (!handWritten.contains(cls) && getClass().getResource('/' + WIRE_DIR + cls.getName() + ".json") == null)
                throw new IllegalStateException(
                    "No wire format description on the classpath [cls=" + cls.getName() + ']');

            lines.add("  \"" + id + "\": \"" + cls.getName() + '"');
        }

        Files.writeString(file, "{\n" + String.join(",\n", lines) + "\n}\n");
    }
}
