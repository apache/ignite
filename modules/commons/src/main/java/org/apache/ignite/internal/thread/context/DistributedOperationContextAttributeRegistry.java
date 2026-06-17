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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class DistributedOperationContextAttributeRegistry {
    /** */
    private static final DistributedOperationContextAttributeRegistry INSTANCE = new DistributedOperationContextAttributeRegistry();

    /** Attributes by their id. */
    private final Map<Byte, OperationContextAttribute<?>> attributes = new ConcurrentHashMap<>();

    /** */
    public static DistributedOperationContextAttributeRegistry instance() {
        return INSTANCE;
    }

    /** */
    public void register(byte id, OperationContextAttribute<?> attr) {
        assert id >= 0;

        if (attributes.size() == OperationContextAttribute.MAX_ATTR_CNT)
            throw new IgniteException("Maximum number of attributes is exceeded [" + OperationContextAttribute.MAX_ATTR_CNT + "].");

        if (attributes.putIfAbsent(id, attr) != null)
            throw new IgniteException("Duplicated attribute id: " + id);
    }

    /**
     * TODO : Declare distributed attributes as 'extends Message' after https://issues.apache.org/jira/browse/IGNITE-28766
     * @return Values for all registered operation context attributes.
     * */
    public <T> Map<Byte, T> collectContext(@Nullable Class<T> checkValuesType) {
        Map<Byte, T> res = Collections.emptyMap();

        for (Map.Entry<Byte, OperationContextAttribute<?>> e : attributes.entrySet()) {
            OperationContextAttribute<?> attr = e.getValue();

            Object curVal = OperationContext.get(attr);

            if (curVal != null && checkValuesType != null && !checkValuesType.isAssignableFrom(curVal.getClass())) {
                throw new IgniteException("To distribute operation context attributes they have to be a "
                    + checkValuesType.getSimpleName());
            }

            if (!Objects.equals(attr.initialValue(), curVal)) {
                if (res == Collections.EMPTY_MAP)
                    res = new HashMap<>(attributes.size(), 1.0f);

                res.put(e.getKey(), (T)curVal);
            }
        }

        return res;
    }

    /** */
    public Scope restoreContext(int idBitmask, Object[] values) {
        if (F.isEmpty(values) || idBitmask == 0)
            return Scope.NOOP_SCOPE;

        OperationContext.ContextUpdater updater = OperationContext.ContextUpdater.create();

        for (byte attrId = 0; attrId < OperationContextAttribute.MAX_ATTR_CNT; attrId++) {
            assert attrId < Integer.SIZE;

            int mask = 1 << attrId;

            if ((mask & idBitmask) == 0)
                continue;

            updater.set((OperationContextAttribute<Object>)attributes.get(attrId), values[attrId]);
        }

        return updater.apply();
    }
}
