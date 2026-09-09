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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory;

/** Exports the actual production registrations and their compiled field descriptions. */
public final class MessageTable {
    /** JSON codec; also used by other Ignite modules. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** No instances. */
    private MessageTable() {
        // No-op.
    }

    /**
     * Exports the registered messages to a JSON file.
     *
     * @param args Output file path.
     * @throws Exception If the build is incomplete or the inputs cannot be read.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1)
            throw new IllegalArgumentException("Expected: output-file");

        Path out = Path.of(args[0]).toAbsolutePath();
        String table = generate();

        Files.createDirectories(out.getParent());
        Files.writeString(out, table);
    }

    /**
     * @return Canonical JSON table.
     * @throws Exception If any registered message cannot be described.
     */
    static String generate() throws Exception {
        MessageFactoryProvider[] providers = {
            new CoreMessagesProvider(),
            new GridH2ValueMessageFactory(),
            new CalciteMessageFactory(),
            new ZkMessageFactory()
        };
        var factory = new IgniteMessageFactoryImpl<>(providers);
        short[] ids = factory.registeredDirectTypes();

        Arrays.sort(ids);

        List<Map<String, Object>> msgs = new ArrayList<>();

        try (MessageSchema schemas = new MessageSchema()) {
            for (short id : ids) {
                var msg = factory.create(id);
                String cls = msg.getClass().getName();

                List<String> schema = schemas.read(msg.getClass());

                msgs.add(new TreeMap<>(Map.of("id", (int)id, "class", cls, "schema", schema)));
            }
        }

        return JSON.writer(new DefaultPrettyPrinter().withArrayIndenter(new DefaultIndenter("  ", "\n"))).writeValueAsString(
            new TreeMap<>(Map.of("formatVersion", 1, "providers",
                Arrays.stream(providers).map(p -> p.getClass().getName()).toList(), "messages", msgs))) + "\n";
    }

}
