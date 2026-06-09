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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.context.OperationContextAttribute.MAX_ATTR_CNT;

/** */
public class DistributedOperationAttributeRegistry {
    /** */
    public static final DistributedOperationAttributeRegistry INSTANCE = new DistributedOperationAttributeRegistry();

    /** */
    private final AtomicInteger idGen = new AtomicInteger();

    /** Attributes by their message type. */
    private final Map<Class<? extends Message>, OperationContextAttribute<? extends Message>> msgAttrs = new ConcurrentHashMap<>();

    /** Attributes by their bitmask. */
    private final Map<Integer, OperationContextAttribute<Message>> bitmaskAttrs = new ConcurrentHashMap<>();

    /** */
    public <T extends Message> OperationContextAttribute<T> register(Class<T> msgType, @Nullable T initVal) {
        assert initVal == null || msgType.isAssignableFrom(initVal.getClass());

        OperationContextAttribute<T> attr = null;

        synchronized (idGen) {
            int id = idGen.getAndIncrement();

            try {
                assert id < MAX_ATTR_CNT 
                    : "Exceeded maximum supported number of created Attributes instances [maxCnt=" + MAX_ATTR_CNT + ']';

                attr = new OperationContextAttribute<>(1 << id, initVal);

                if (msgAttrs.putIfAbsent(msgType, attr) != null) {
                    throw new IgniteException("Attribute with distributed id " + id + " and message type "
                        + msgType.getSimpleName() + " already exists.");
                }
            }
            catch (Throwable t) {
                idGen.decrementAndGet();
            }
        }

        bitmaskAttrs.put(attr.bitmask(), (OperationContextAttribute<Message>)attr);

        return attr;
    }

    /** */
    public static @Nullable OperationContextAttribute<Message> attribute(int bitmask) {
        return INSTANCE.bitmaskAttrs.get(bitmask);
    }

    /** */
    public static @Nullable <T extends Message> OperationContextAttribute<T> attribute(Class<T> msgType) {
        return (OperationContextAttribute<T>)INSTANCE.msgAttrs.get(msgType);
    }

    /** */
    public static Message attributeMessage(OperationContextAttribute<?> attr, @Nullable Object val) {
        if (val == null)
            return null;

        assert val instanceof Message
            : "Distributed context attribute value have to be a Message. Current type is: " + val.getClass().getSimpleName();

        assert attr.initialValue() == null || attr.initialValue().getClass() == val.getClass();

        return (Message)val;
    }
}
