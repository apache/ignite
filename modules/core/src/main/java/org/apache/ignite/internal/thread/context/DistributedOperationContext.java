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

import java.util.Arrays;
import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.context.OperationContextSnapshotMessage.nextAttributeId;
import static org.apache.ignite.internal.thread.context.OperationContextSnapshotMessage.set;

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
 * <p>To enable propagation of an {@link OperationContextAttribute} value across cluster nodes, the attribute must be
 * registered with the {@link #register(DistributedAttributeKey, OperationContextAttribute)}
 * method.
 *
 * <p> Note, that the maximum number of distributed attributes to register is currently limited to
 * {@link OperationContextSnapshotMessage#MAX_ATTRS_CNT} for implementation reasons.</p>
 *
 * @see OperationContext
 * @see OperationContextSnapshotMessage
 */
public class DistributedOperationContext {
    /** Registered distributed attributes by their cluster-wide id. */
    private volatile DistributedAttribute[] distrAttrs = new DistributedAttribute[0];

    /** Whether the registration of new distributed attributes is allowed. */
    private boolean regFinished;

    /**
     * Registers an attribute of {@link OperationContext} with the specified distributed attribute key.
     *
     * <p>The key consistently identifies the attribute across all nodes in the cluster and must be unique.</p>
     *
     * <p>Registered attribute value is automatically captured and propagated between cluster nodes
     * during the messages transmission.</p>
     *
     * @see DistributedAttributeKey
     */
    public synchronized <T extends Message> void register(DistributedAttributeKey key, OperationContextAttribute<T> attr) {
        if (regFinished)
            throw new IgniteException("Initialization of distributed operation context attributes has already finished.");

        byte id = key.id();

        if (key.equals(key(id)))
            throw new IgniteException("Duplicated distributed attribute id [id=" + id + ']');

        DistributedAttribute[] locDistrAttrs = distrAttrs;

        DistributedAttribute[] copy = Arrays.copyOf(locDistrAttrs, Math.max(locDistrAttrs.length, id + 1));

        copy[id] = new DistributedAttribute(key, attr);

        distrAttrs = copy;
    }

    /** @return Key of the attribute registered under the id, or {@code null} if none is. */
    public @Nullable DistributedAttributeKey key(int id) {
        DistributedAttribute[] attrs = distrAttrs;

        if (id < 0 || id >= attrs.length)
            return null;

        DistributedAttribute attr = attrs[id];

        return attr == null ? null : attrs[id].key;
    }

    /**
     * Collects the values of all registered distributed {@link OperationContextAttribute}s.
     *
     * @see OperationContext#get(OperationContextAttribute)
     */
    public @Nullable OperationContextSnapshotMessage createSnapshot() {
        DistributedAttribute[] locDistrAttrs = distrAttrs;

        Message[] attrs = null;

        byte idBitmap = 0;
        int cnt = 0;

        for (int id = 0; id < locDistrAttrs.length; id++) {
            if (locDistrAttrs[id] == null)
                continue;

            OperationContextAttribute<? extends Message> attr = locDistrAttrs[id].attr;

            Message curVal = OperationContext.get(attr);

            if (curVal == attr.initialValue())
                continue;

            if (attrs == null)
                attrs = new Message[locDistrAttrs.length - id];

            attrs[cnt++] = curVal;

            idBitmap = set(idBitmap, id);
        }

        if (attrs == null)
            return null;

        return new OperationContextSnapshotMessage(idBitmap, cnt == attrs.length ? attrs : Arrays.copyOf(attrs, cnt));
    }

    /** Restores {@link OperationContextAttribute} values received from a remote node. */
    public Scope restoreSnapshot(@Nullable OperationContextSnapshotMessage snp) {
        if (snp == null)
            return OperationContext.Restorer.restoreEmpty();

        DistributedAttribute[] locDistrAttrs = distrAttrs;

        OperationContext.Restorer ctxRestorer = OperationContext.Restorer.create();

        for (int attrId = nextAttributeId(snp.idBitmap, -1); attrId >= 0; attrId = nextAttributeId(snp.idBitmap, attrId)) {
            assert attrId < locDistrAttrs.length;
            assert locDistrAttrs[attrId] != null;

            ctxRestorer.add((OperationContextAttribute<Message>)locDistrAttrs[attrId].attr, snp.getValue(attrId));
        }

        return ctxRestorer.restore();
    }

    /** Restricts further registration of distributed attributes. */
    public synchronized void finishRegistration() {
        regFinished = true;
    }

    /** */
    private record DistributedAttribute(DistributedAttributeKey key, OperationContextAttribute<? extends Message> attr) {}
}
