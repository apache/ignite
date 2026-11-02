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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.managers.communication.CompressedMessage;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactory;
import org.apache.ignite.internal.plugin.AbstractMessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** */
public class MessageIdsWriterTest {
    /** */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** */
    @Test public void testWrite() throws IOException {
        Path dir = tmp.getRoot().toPath();

        createTestMessageDescription(dir);

        new MessageIdsWriter(providers(false)).write(dir);

        String ids = Files.readString(dir.resolve("ids.json"));

        assertTrue(ids, ids.contains("\"42\": \"" + TestMessage.class.getName() + '"'));
    }

    /** */
    @Test public void testMissingDescription() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> new MessageIdsWriter(providers(false)).write(tmp.getRoot().toPath()));

        assertTrue(e.getMessage(), e.getMessage().contains(TestMessage.class.getName()));
    }

    /** */
    @Test public void testHandWrittenMessageWithoutDescription() throws IOException {
        Path dir = tmp.getRoot().toPath();

        createTestMessageDescription(dir);

        new MessageIdsWriter(providers(true)).write(dir);

        try (InputStream in = MessageIdsWriterTest.class.getResourceAsStream("expected-ids.json")) {
            String expected = new String(in.readAllBytes(), StandardCharsets.UTF_8);

            assertEquals(expected, Files.readString(dir.resolve("ids.json")));
        }
    }

    /** Creates the description for the ordinary test message. */
    private static void createTestMessageDescription(Path dir) throws IOException {
        Path msgs = Files.createDirectories(dir.resolve("messages"));

        Files.writeString(msgs.resolve(TestMessage.class.getName() + ".json"), "{}");
    }

    /** @param includeCompressed Whether to register the hand-written message as well. */
    private static MessageFactoryProvider[] providers(boolean includeCompressed) {
        return new MessageFactoryProvider[] {
            new AbstractMessageFactoryProvider() {
                @Override public void registerAll(IgniteMessageFactory factory) {
                    if (includeCompressed)
                        register(factory, CompressedMessage.class, (short)5000);

                    register(factory, TestMessage.class, (short)42);
                }
            }
        };
    }
}
