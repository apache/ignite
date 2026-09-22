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
 * registered with the {@link #registerDistributedAttribute(DistributedAttributeKey, OperationContextAttribute)}
 * method.
 *
 * <p> Note, that the maximum number of distributed attributes to register is currently limited to
 * {@link #MAX_ATTRS_CNT} for implementation reasons.</p>
 *
 * @see OperationContext
 * @see OperationContextSnapshotMessage
 */
public class OperationContextDispatcher {
    /** Maximal number of supported distributed attributes. */
    static final byte MAX_ATTRS_CNT = Byte.SIZE;

    /** Registered distributed attributes by their cluster-wide id. */
    private volatile OperationContextAttribute<? extends Message>[] registeredAttrs = new OperationContextAttribute[0];

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
     * @see DistributedAttributeKeyRegistry
     */
    public synchronized <T extends Message> void registerDistributedAttribute(
        DistributedAttributeKey key,
        OperationContextAttribute<T> attr
    ) {
        if (regFinished)
            throw new IgniteException("Initialization of distributed operation context attributes has already finished.");

        assert DistributedAttributeKeyRegistry.get(key.id()) == key;

        byte id = key.id();

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
    public @Nullable OperationContextSnapshotMessage createSnapshot() {
        OperationContextAttribute<? extends Message>[] locRegisteredAttrs = registeredAttrs;

        if (locRegisteredAttrs.length == 0)
            return null;

        Message[] attrs = new Message[locRegisteredAttrs.length];

        byte idBitmap = 0;
        int cnt = 0;

        for (int id = 0; id < locRegisteredAttrs.length; id++) {
            OperationContextAttribute<? extends Message> attr = locRegisteredAttrs[id];

            if (attr == null)
                continue;

            Message curVal = OperationContext.get(attr);

            if (curVal == attr.initialValue())
                continue;

            attrs[cnt++] = curVal;

            idBitmap = set(idBitmap, id);
        }

        if (idBitmap == 0)
            return null;

        return new OperationContextSnapshotMessage(idBitmap, cnt == attrs.length ? attrs : Arrays.copyOf(attrs, cnt));
    }

    /** Restores {@link OperationContextAttribute} values received from a remote node. */
    public Scope restoreSnapshot(@Nullable OperationContextSnapshotMessage snp) {
        if (snp == null)
            return OperationContext.Restorer.restoreEmpty();

        OperationContextAttribute<? extends Message>[] locRegisteredAttrs = registeredAttrs;

        OperationContext.Restorer ctxRestorer = OperationContext.Restorer.create();

        for (byte attrId = 0, valIdx = 0; attrId < MAX_ATTRS_CNT && valIdx < snp.attrs.length; ++attrId) {
            if (!contains(snp.idBitmap, attrId))
                continue;

            Message attrVal = snp.attrs[valIdx++];

            assert attrId < locRegisteredAttrs.length;

            OperationContextAttribute<Message> attr = (OperationContextAttribute<Message>)locRegisteredAttrs[attrId];

            assert attr != null;

            ctxRestorer.add(attr, attrVal);
        }

        return ctxRestorer.restore();
    }

    /** Restricts further registration of distributed attributes. */
    public synchronized void finishRegistration() {
        regFinished = true;
    }

    /** @return Number of distributed attributes the bitmap of their ids names. */
    static int attributesCount(byte idBitmap) {
        return Integer.bitCount(idBitmap & 0xFF);
    }

    /** @return Whether the bitmap names the distributed attribute with the specified id. */
    static boolean contains(byte idBitmap, int attrId) {
        return (idBitmap & (1 << attrId)) != 0;
    }

    /** @return The bitmap with the distributed attribute with the specified id added. */
    static byte set(byte idBitmap, int attrId) {
        return (byte)(idBitmap | (1 << attrId));
    }
}
