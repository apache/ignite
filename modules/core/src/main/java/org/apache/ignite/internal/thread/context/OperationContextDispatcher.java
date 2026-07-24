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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteException;
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
 * attribute must be registered with the {@link #registerDistributedAttribute(int, OperationContextAttribute)} method.
 *
 * <p> Note, that the maximum number of distributed attributes to register is currently limited to
 * {@link #MAX_ATTRS_CNT} for implementation reasons.</p>
 *
 * @see OperationContext
 * @see OperationContextMessage
 */
public class OperationContextDispatcher {
    /** Maximal number of supported distributed attributes. */
    static final byte MAX_ATTRS_CNT = Byte.SIZE;

    /** Registered distributed attributes by their cluster-wide id. */
    private volatile OperationContextAttribute<? extends Message>[] registeredAttrs = new OperationContextAttribute[0];

    /** Whether the registration of new distributed attributes is allowed. */
    private boolean regFinished;

    /**
     * Registers an attribute of {@link OperationContext} with the specified distributed ID.
     *
     * <p>The distributed ID is used to consistently identify the attribute across all nodes in the cluster.
     * It must be unique, and its value must be in the range [{@code 0} : {@code Byte.SIZE}).</p>
     *
     * <p>Registered attribute value is automatically captured and propagated between cluster nodes
     * during the messages transmission.</p>
     *
     * @see DistributedAttributeRegistry
     */
    public synchronized <T extends Message> void registerDistributedAttribute(int id, OperationContextAttribute<T> attr) {
        if (regFinished)
            throw new IgniteException("Initialization of distributed operation context attributes has already finished.");

        assert 0 <= id && id < MAX_ATTRS_CNT : "Invalid distributed attributed id [id=" + id + ']';

        OperationContextAttribute<? extends Message>[] locRegisteredAttrs = registeredAttrs;

        OperationContextAttribute<? extends Message>[] copy = Arrays.copyOf(
            locRegisteredAttrs,
            Math.max(locRegisteredAttrs.length, id + 1));

        if (copy[id] != null)
            throw new IgniteException("Duplicated distributed attribute id [id=" + id + ']');

        copy[id] = attr;

        registeredAttrs = copy;
    }

    /**
     * Collects the values of all distributed {@link OperationContextAttribute}s registered by this dispatcher.
     *
     * @see OperationContext#get(OperationContextAttribute)
     */
    public @Nullable OperationContextMessage collectDistributedAttributeValues() {
        OperationContextAttribute<? extends Message>[] locRegisteredAttrs = registeredAttrs;

        if (locRegisteredAttrs.length == 0)
            return null;

        byte bitmap = 0;
        List<Message> vals = null;

        for (int id = 0; id < locRegisteredAttrs.length; id++) {
            OperationContextAttribute<? extends Message> attr = locRegisteredAttrs[id];

            if (attr == null)
                continue;

            Message curVal = OperationContext.get(attr);

            if (curVal == attr.initialValue())
                continue;

            if (vals == null)
                vals = new ArrayList<>(MAX_ATTRS_CNT / 2);

            byte mask = (byte)(1 << id);

            assert (bitmap & mask) == 0;

            vals.add(curVal);
            bitmap |= mask;
        }

        return bitmap == 0 ? null : new OperationContextMessage(bitmap, vals.toArray(Message[]::new));
    }

    /** Restores distributed {@link OperationContextAttribute} values received from a remote node. */
    public Scope restoreRemoteAttributeValues(@Nullable OperationContextMessage msg) {
        if (msg == null)
            return Scope.NOOP_SCOPE;

        OperationContextAttribute<? extends Message>[] locRegisteredAttrs = registeredAttrs;

        assert msg.idBitmap != 0;
        assert !F.isEmpty(msg.attrs);
        assert msg.attrs.length <= MAX_ATTRS_CNT;

        OperationContext.ContextUpdater updater = OperationContext.ContextUpdater.create();

        for (byte valIdx = 0, attrId = 0; valIdx < msg.attrs.length; ++valIdx) {
            Message curVal = msg.attrs[valIdx];

            while ((msg.idBitmap & (1 << attrId)) == 0)
                ++attrId;

            assert attrId < locRegisteredAttrs.length;

            OperationContextAttribute<Message> attr = (OperationContextAttribute<Message>)locRegisteredAttrs[attrId++];

            assert attr != null;

            updater.set(attr, curVal);
        }

        return updater.apply();
    }

    /** Restricts further registration of distributed attributes. */
    public synchronized void finishRegistration() {
        regFinished = true;
    }
}
