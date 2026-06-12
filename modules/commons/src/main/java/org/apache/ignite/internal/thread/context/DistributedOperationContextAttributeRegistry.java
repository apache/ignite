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
package org.apache.ignite.internal.thread.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class DistributedOperationContextAttributeRegistry {
    /** */
    private static final DistributedOperationContextAttributeRegistry INSTANCE = new DistributedOperationContextAttributeRegistry();

    /** Attributes by their id. */
    private final Map<Byte, OperationContextAttribute<? extends Message>> attributes = new ConcurrentHashMap<>();

    /** */
    public static DistributedOperationContextAttributeRegistry instance() {
        return INSTANCE;
    }

    /** */
    public <T extends Message> void register(byte id, OperationContextAttribute<T> attr) {
        assert id >= 0;

        if(attributes.size() == OperationContextAttribute.MAX_ATTR_CNT)
            throw new IgniteException("Maximum number of attributes is exceeded [" + OperationContextAttribute.MAX_ATTR_CNT + "].");

        if (attributes.putIfAbsent(id, attr) != null)
            throw new IgniteException("Duplicated attribute id: " + id);
    }

    /** @return Values for all registered operation context attributes. */
    public @Nullable Map<Byte, Message> collectContext() {
        Map<Byte, Message> res = null;

        for (Map.Entry<Byte, OperationContextAttribute<? extends Message>> e : attributes.entrySet()) {
            OperationContextAttribute<? extends Message> attr = e.getValue();

            Message curVal = OperationContext.get(attr);

            if (!Objects.equals(attr.initialValue(), curVal)) {
                if (res == null)
                    res = new HashMap<>(attributes.size(), 1.0f);

                res.put(e.getKey(), curVal);
            }
        }

        return res;
    }

    /** */
    public Scope restoreContext(Map<Byte, Message> res) {
        if (F.isEmpty(res))
            return Scope.NOOP_SCOPE;

        OperationContext.ContextUpdater updater = OperationContext.ContextUpdater.create();

        res.forEach((id, attr) -> updater.set((OperationContextAttribute<Message>)attributes.get(id), attr));

        return updater.apply();
    }
}
