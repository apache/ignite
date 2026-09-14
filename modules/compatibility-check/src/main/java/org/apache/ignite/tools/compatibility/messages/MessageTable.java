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
import java.util.List;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory;
import org.apache.ignite.tools.compatibility.messages.dto.MessageRepresentation;

/** Exports the actual production registrations and their compiled field descriptions. */
public class MessageTable {
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
        MessageFactoryProvider[] providers = {
            new CoreMessagesProvider(),
            new GridH2ValueMessageFactory(),
            new CalciteMessageFactory(),
            new ZkMessageFactory()
        };

        List<MessageRepresentation> msgs = new MessageTableCollector(providers).collect();
        String table = new XmlTableWriter().toXml(providers, msgs);

        Files.createDirectories(out.getParent());
        Files.writeString(out, table);
    }
}
