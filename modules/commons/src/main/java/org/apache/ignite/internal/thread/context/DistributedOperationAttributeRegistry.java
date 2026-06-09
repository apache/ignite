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

import java.util.Collection;
import java.util.Collections;
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
    private static final DistributedOperationAttributeRegistry INSTANCE = new DistributedOperationAttributeRegistry();

    /** */
    private final AtomicInteger bitmaskGen = new AtomicInteger();

    /** Attributes by message type. */
    private final Map<Class<? extends Message>, OperationContextAttribute<? extends Message>> msgAttrs = new ConcurrentHashMap<>();

    /** Attributes by message id. */
    private final Map<Byte, OperationContextAttribute<? extends Message>> idAttrs = new ConcurrentHashMap<>();

    /** Attributes mapping: bitmask -> id */
    private final Map<Integer, Byte> attrsMaskIdMap = new ConcurrentHashMap<>();

    /** */
    public static DistributedOperationAttributeRegistry get() {
        return INSTANCE;
    }

    /** */
    public <T extends Message> OperationContextAttribute<T> register(int id, Class<T> msgType, @Nullable T initVal) {
        assert id >= 0;
        assert initVal == null || msgType.isAssignableFrom(initVal.getClass());

        OperationContextAttribute<T> attr;

        synchronized (bitmaskGen) {
            try {
                assert msgAttrs.size() < MAX_ATTR_CNT
                    : "Exceeded maximum number of created Attributes instances [maxCnt=" + MAX_ATTR_CNT + ']';
                assert id < MAX_ATTR_CNT : "Exceeded maximum attribute id " + (MAX_ATTR_CNT - 1);

                byte id0 = (byte)id;

                int bitmask = 1 << bitmaskGen.getAndIncrement();

                assert attrsMaskIdMap.get(bitmask) == null;
                assert idAttrs.get(id0) == null;

                attr = new OperationContextAttribute<>(bitmask, initVal);

                if (msgAttrs.putIfAbsent(msgType, attr) != null)
                    throw new IgniteException("Attribute with message type " + msgType.getSimpleName() + " already exists.");

                attrsMaskIdMap.put(bitmask, id0);
                idAttrs.put(id0, attr);
            }
            catch (Throwable t) {
                bitmaskGen.decrementAndGet();

                throw t;
            }
        }

        return attr;
    }

    /** */
    public Collection<OperationContextAttribute<? extends Message>> attributes() {
        return Collections.unmodifiableCollection(msgAttrs.values());
    }

    /** */
    public @Nullable <T extends Message> OperationContextAttribute<T> attribute(Class<T> msgType) {
        return (OperationContextAttribute<T>)INSTANCE.msgAttrs.get(msgType);
    }

    /** */
    public @Nullable <T extends Message> OperationContextAttribute<T> attribute(byte attrId) {
        return (OperationContextAttribute<T>)INSTANCE.idAttrs.get(attrId);
    }

    /** */
    public @Nullable Byte attributeId(OperationContextAttribute<? extends Message> attr) {
        return attrsMaskIdMap.get(attr.bitmask());
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

    /** Mostly for testing purposes. */
    public void clear() {
        msgAttrs.clear();
        attrsMaskIdMap.clear();
        bitmaskGen.set(0);
    }
}
