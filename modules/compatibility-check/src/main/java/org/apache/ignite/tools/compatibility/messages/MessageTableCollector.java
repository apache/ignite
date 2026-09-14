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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.tools.compatibility.messages.dto.MessageRepresentation;

/** Collects registered messages and their compiled schemas. */
class MessageTableCollector {
    /** Factory with the selected providers registered. */
    private final IgniteMessageFactoryImpl<?, ?> factory;

    /** @param providers Message registration providers. */
    MessageTableCollector(MessageFactoryProvider[] providers) {
        factory = new IgniteMessageFactoryImpl<>(providers);
    }

    /**
     * @return Collected message table.
     * @throws Exception If any registered message cannot be described.
     */
    List<MessageRepresentation> collect() throws IOException {
        short[] ids = factory.registeredDirectTypes();

        Arrays.sort(ids);

        List<MessageRepresentation> msgs = new ArrayList<>();

        try (MessageSchemaReader schemas = new MessageSchemaReader()) {
            for (short id : ids) {
                Class<?> msgCls = factory.create(id).getClass();

                msgs.add(new MessageRepresentation(id, msgCls.getName(), schemas.read(msgCls)));
            }
        }

        return msgs;
    }
}
