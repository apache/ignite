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

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.DistributedOperationAttributesMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class DistributedOperationAttributeManager {
    /** */
    public static final byte MAX_DISTRIBUTED_ATTR_ID = 7;

    /** */
    private static final DistributedOperationAttributeManager INSTANCE = new DistributedOperationAttributeManager();

    /** Attributes by their id. */
    private final Map<Byte, OperationContextAttribute<? extends Message>> attrs = new ConcurrentHashMap<>();

    /** */
    public static DistributedOperationAttributeManager instance() {
        return INSTANCE;
    }

    /** */
    public <T extends Message> OperationContextAttribute<T> createDistributedAttriubte(byte id, @Nullable T initVal) {
        assert id >= 0;

        if (attrs.size() == OperationContextAttribute.MAX_ATTR_CNT)
            throw new IgniteException("Maximum number of ributed attributes is exceeded [" + OperationContextAttribute.MAX_ATTR_CNT + "].");

        attrs.compute(id, (id0, attr0) -> {
            if (attr0 != null)
                throw new IgniteException("Duplicated distributed attribute id: " + id);

            return OperationContextAttribute.newInstance(initVal);
        });

        return (OperationContextAttribute<T>)attrs.get(id);
    }

    /** */
    public @Nullable DistributedOperationAttributesMessage collectDistributedAttributes() {
        DistributedOperationAttributesMessage res = null;

        for (Map.Entry<Byte, OperationContextAttribute<? extends Message>> e : attrs.entrySet()) {
            OperationContextAttribute<? extends Message> attr = e.getValue();

            Message curVal = OperationContext.get(attr);

            assert attr.initialValue() == null || curVal == null || curVal.getClass().isAssignableFrom(attr.initialValue().getClass());

            if (!Objects.equals(curVal, attr.initialValue())) {
                if (res == null) {
                    res = new DistributedOperationAttributesMessage();

                    res.vals = new ArrayList<>(MAX_DISTRIBUTED_ATTR_ID / 2);
                }

                byte mask = (byte)(1 << e.getKey());

                assert (res.idBitmask & mask) == 0;

                res.vals.add(curVal);
                res.idBitmask |= mask;
            }
        }

        return res;
    }

    /** */
    public Scope restoreDistributedAttributes(@Nullable DistributedOperationAttributesMessage msg) {
        if (msg == null)
            return Scope.NOOP_SCOPE;

        assert msg.idBitmask != 0;
        assert !F.isEmpty(msg.vals);
        assert msg.vals.size() <= MAX_DISTRIBUTED_ATTR_ID;

        OperationContext.ContextUpdater updater = OperationContext.ContextUpdater.create();

        for (byte valIdx = 0, maskIdx = -1; valIdx < msg.vals.size(); ++valIdx) {
            Message curVal = msg.vals.get(valIdx);

            while (maskIdx < 0 || (msg.idBitmask & (1 << maskIdx)) == 0) {
                assert maskIdx <= MAX_DISTRIBUTED_ATTR_ID;

                ++maskIdx;
            }

            updater.set((OperationContextAttribute<Message>)attrs.get(maskIdx++), curVal);
        }

        return updater.apply();
    }
}
