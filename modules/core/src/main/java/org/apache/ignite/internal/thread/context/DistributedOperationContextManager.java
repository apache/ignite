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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.DistributedOperationContextMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class DistributedOperationContextManager {
    /** */
    static final byte MAX_DISTRIBUTED_ATTR_CNT = Byte.SIZE;

    /** */
    private static final DistributedOperationContextManager INSTANCE = new DistributedOperationContextManager();

    /** Attributes by their id. */
    private final Map<Byte, OperationContextAttribute<? extends Message>> attrs = new ConcurrentSkipListMap<>();

    /** */
    public static DistributedOperationContextManager instance() {
        return INSTANCE;
    }

    /** */
    public <T extends Message> OperationContextAttribute<T> createDistributedAttribute(byte id, @Nullable T initVal) {
        assert id >= 0 && id < MAX_DISTRIBUTED_ATTR_CNT : "Invalid distributed attributed id [id=" + id + "].";

        if (attrs.size() == MAX_DISTRIBUTED_ATTR_CNT)
            throw new IgniteException("Maximum number of distributed attributes is exceeded [max=" + MAX_DISTRIBUTED_ATTR_CNT + "].");

        return (OperationContextAttribute<T>)attrs.compute(id, (id0, attr0) -> {
            if (attr0 != null)
                throw new IgniteException("Duplicated distributed attribute id [id=" + id + "].");

            return OperationContextAttribute.newInstance(initVal);
        });
    }

    /** */
    public @Nullable DistributedOperationContextMessage collectDistributedAttributes() {
        DistributedOperationContextMessage res = null;
        List<Message> vals = null;

        for (Map.Entry<Byte, OperationContextAttribute<? extends Message>> e : attrs.entrySet()) {
            OperationContextAttribute<? extends Message> attr = e.getValue();

            Message curVal = OperationContext.get(attr);

            if (curVal != attr.initialValue()) {
                if (res == null) {
                    res = new DistributedOperationContextMessage();

                    vals = new ArrayList<>(MAX_DISTRIBUTED_ATTR_CNT / 2);
                }

                byte mask = (byte)(1 << e.getKey());

                assert (res.idBitmap & mask) == 0;

                vals.add(curVal);
                res.idBitmap |= mask;
            }
        }

        if (res != null)
            res.vals = vals.toArray(vals.toArray(new Message[vals.size()]));

        return res;
    }

    /** */
    public Scope restoreDistributedAttributes(@Nullable DistributedOperationContextMessage msg) {
        if (msg == null)
            return Scope.NOOP_SCOPE;

        assert msg.idBitmap != 0;
        assert !F.isEmpty(msg.vals);
        assert msg.vals.length <= MAX_DISTRIBUTED_ATTR_CNT;

        OperationContext.ContextUpdater updater = OperationContext.ContextUpdater.create();

        for (byte valIdx = 0, maskIdx = 0; valIdx < msg.vals.length; ++valIdx) {
            Message curVal = msg.vals[valIdx];

            while ((msg.idBitmap & (1 << maskIdx)) == 0)
                ++maskIdx;

            updater.set((OperationContextAttribute<Message>)attrs.get(maskIdx++), curVal);
        }

        return updater.apply();
    }
}
