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
import java.util.Set;
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
    /** Expected {@code ids.json}. */
    private static final String IDS = "{\n  \"42\": \"" + TestMessage.class.getName() + "\"\n}\n";

    /** */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** */
    @Test public void testWrite() throws IOException {
        Path dir = tmp.getRoot().toPath();

        Files.createDirectories(dir.resolve("messages"));
        Files.writeString(dir.resolve("messages/" + TestMessage.class.getName() + ".json"), "{}");

        new MessageIdsWriter(providers(), Set.of()).write(dir);

        assertEquals(IDS, Files.readString(dir.resolve("ids.json")));
    }

    /** */
    @Test public void testMissingDescription() throws IOException {
        Path dir = tmp.getRoot().toPath();

        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> new MessageIdsWriter(providers(), Set.of()).write(dir));

        assertTrue(e.getMessage(), e.getMessage().contains(TestMessage.class.getName()));

        new MessageIdsWriter(providers(), Set.of(TestMessage.class)).write(dir);

        assertEquals(IDS, Files.readString(dir.resolve("ids.json")));
    }

    /** @return Provider registering {@link TestMessage} under the direct type 42. */
    private static MessageFactoryProvider[] providers() {
        return new MessageFactoryProvider[] {
            new AbstractMessageFactoryProvider() {
                @Override public void registerAll(IgniteMessageFactory factory) {
                    register(factory, TestMessage.class, (short)42);
                }
            }
        };
    }
}
