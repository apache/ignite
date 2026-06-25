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
import org.apache.ignite.internal.OperationContextMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Provides the ability to manage {@link OperationContext} attributes in a distributed manner.
 *
 * <p>This mechanism is primarily used to propagate {@link OperationContext} state across the cluster by
 * capturing it before a message is sent, transferring it together with the message, and restoring it on
 * the receiving node before message processing begins.</p>
 *
 * <p>The implementation relies on a mapping between a distributed identifier and an
 * {@link OperationContextAttribute} instance that is consistent across all cluster nodes.</p>
 *
 * <p>To enable propagation of an {@link OperationContextAttribute} value across cluster nodes, the
 * attribute must be registered via the {@link #registerDistributedAttribute(byte, OperationContextAttribute)} method.
 *
 * <p> Note, that the maximum number of distributed attributes to register is currently limited to
 * {@link #MAX_DISTRIBUTED_ATTR_CNT} for implementation reasons.</p>
 *
 * @see OperationContext
 * @see OperationContextMessage
 */
public class OperationContextDispatcher {
    /** Maximal number of supported distributed attributes. */
    static final byte MAX_DISTRIBUTED_ATTR_CNT = Byte.SIZE;

    /** Registered distributed attributes by their cluster-wide id. */
    private final Map<Byte, OperationContextAttribute<? extends Message>> attrs = new ConcurrentSkipListMap<>();

    /** Whether the registration of new distributed attributes is allowed. */
    private volatile boolean registrationFinished;

    /**
     * Registers an attribute of {@link OperationContext} with the specified distributed ID.
     *
     * <p>The distributed ID is used to consistently identify the attribute across all nodes in the cluster.
     * It must be unique, and its value must be in the range [{@code 0} : {@code Byte.SIZE}).</p>
     *
     * <p>Registered attribute value is automatically captured and propagated between cluster nodes
     * during the messages transmission.</p>
     */
    public <T extends Message> void registerDistributedAttribute(byte id, OperationContextAttribute<T> attr) {
        if (registrationFinished)
            throw new IgniteException("Initialization of distributed operation context attributes has already finished.");

        assert id >= 0 && id < MAX_DISTRIBUTED_ATTR_CNT : "Invalid distributed attributed id [id=" + id + ']';

        if (attrs.putIfAbsent(id, attr) != null)
            throw new IgniteException("Duplicated distributed attribute id [id=" + id + ']');
    }

    /**
     * Collects the values of all distributed {@link OperationContextAttribute}s registered by this dispatcher.
     *
     * @see OperationContext#get(OperationContextAttribute)
     */
    public @Nullable OperationContextMessage collectDistributedAttributes() {
        OperationContextMessage res = null;
        List<Message> vals = null;

        for (Map.Entry<Byte, OperationContextAttribute<? extends Message>> e : attrs.entrySet()) {
            OperationContextAttribute<? extends Message> attr = e.getValue();

            Message curVal = OperationContext.get(attr);

            if (curVal != attr.initialValue()) {
                if (res == null) {
                    res = new OperationContextMessage();

                    vals = new ArrayList<>(MAX_DISTRIBUTED_ATTR_CNT / 2);
                }

                byte mask = (byte)(1 << e.getKey());

                assert (res.idBitmap & mask) == 0;

                vals.add(curVal);
                res.idBitmap |= mask;
            }
        }

        if (res != null)
            res.vals = vals.toArray(new Message[vals.size()]);

        return res;
    }

    /** Restores distributed {@link OperationContextAttribute} values received from a remote node. */
    public Scope restoreDistributedAttributes(@Nullable OperationContextMessage msg) {
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

            OperationContextAttribute<Message> attr = (OperationContextAttribute<Message>)attrs.get(maskIdx++);

            assert attr != null;

            updater.set(attr, curVal);
        }

        return updater.apply();
    }

    /** Restricts further registration of distributed attributes. */
    public void finishRegistration() {
        registrationFinished = true;
    }
}
