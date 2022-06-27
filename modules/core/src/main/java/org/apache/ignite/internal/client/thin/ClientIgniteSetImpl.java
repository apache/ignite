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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Client Ignite Set.
 */
class ClientIgniteSetImpl<T> implements ClientIgniteSet<T> {
    private final String name;

    private final IgniteUuid id;

    private final ReliableChannel ch;

    private final ClientUtils serDes;

    private final boolean colocated;

    private final int cacheId;

    private volatile boolean removed;

    private volatile boolean serverKeepBinary = true;

    /**
     * Constructor.
     * @param ch Channel.
     * @param serDes Utils..
     * @param name Name.
     * @param id Id.
     * @param colocated Colocated flag.
     * @param cacheId Cache id.
     */
    public ClientIgniteSetImpl(
            ReliableChannel ch,
            ClientUtils serDes,
            String name,
            IgniteUuid id,
            boolean colocated,
            int cacheId) {
        assert ch != null;
        assert serDes != null;
        assert name != null;
        assert id != null;

        this.ch = ch;
        this.serDes = serDes;
        this.name = name;
        this.id = id;
        this.colocated = colocated;
        this.cacheId = cacheId;
    }

    /** {@inheritDoc} */
    @Override public boolean add(T o) {
        A.notNull(o, "o");

        return singleKeyOp(ClientOperation.OP_SET_VALUE_ADD, o);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        A.notNull(c, "c");

        return multiKeyOp(ClientOperation.OP_SET_VALUE_ADD_ALL, c);
    }

    @Override
    public void clear() {
        op(ClientOperation.OP_SET_CLEAR, null, null);
    }

    @Override
    public boolean contains(Object o) {
        A.notNull(o, "o");

        return singleKeyOp(ClientOperation.OP_SET_VALUE_CONTAINS, o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        A.notNull(c, "c");

        return multiKeyOp(ClientOperation.OP_SET_VALUE_CONTAINS_ALL, c);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<T> iterator() {
        ch.service(ClientOperation.OP_SET_ITERATOR_START, this::writeIdentity, in -> {
            // Read first page, hasNext and resId.
            int cnt = in.in().readInt();
        });

        return null;
    }

    @Override
    public boolean remove(Object o) {
        A.notNull(o, "o");

        return singleKeyOp(ClientOperation.OP_SET_VALUE_REMOVE, o);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        A.notNull(c, "c");

        return multiKeyOp(ClientOperation.OP_SET_VALUE_REMOVE_ALL, c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        A.notNull(c, "c");

        if (c.isEmpty()) {
            // Special case: remove all.
            // Not the same as clear, because we need the boolean result.
            return ch.service(ClientOperation.OP_SET_VALUE_RETAIN_ALL, out -> {
                try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
                    writeIdentity(w);
                    w.writeBoolean(serverKeepBinary);
                    w.writeInt(0); // Size.
                }
            }, r -> r.in().readBoolean());
        }

        return multiKeyOp(ClientOperation.OP_SET_VALUE_RETAIN_ALL, c);
    }

    @Override
    public int size() {
        return op(ClientOperation.OP_SET_SIZE, null, r -> r.in().readInt());
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return null;
    }

    @Override
    public void close() {
        if (removed)
            return;

        op(ClientOperation.OP_SET_CLOSE, null, null);

        removed = true;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean colocated() {
        return colocated;
    }

    @Override
    public boolean removed() {
        if (removed)
            return true;

        removed = !op(ClientOperation.OP_SET_EXISTS, null, r -> r.in().readBoolean());

        return removed;
    }

    /** {@inheritDoc} */
    @Override public ClientIgniteSet<T> serverKeepBinary(boolean keepBinary) {
        serverKeepBinary = keepBinary;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean serverKeepBinary() {
        return serverKeepBinary;
    }

    private Boolean singleKeyOp(ClientOperation op, Object key) {
        Object affKey = affinityKey(key);

        return ch.affinityService(cacheId, affKey, op, out -> {
            try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
                writeIdentity(w);

                w.writeBoolean(serverKeepBinary);
                w.writeObject(key);
            }
        }, r -> r.in().readBoolean());
    }

    @SuppressWarnings("rawtypes")
    private Boolean multiKeyOp(ClientOperation op, Collection keys) {
        if (keys.isEmpty())
            return false;

        Iterator iter = keys.iterator();
        Object firstKey = iter.next();

        // Use the first key as affinity key as a simple optimization.
        // Let the server map other keys to nodes, while still using no more than N network requests.
        Object affKey = affinityKey(firstKey);

        return ch.affinityService(cacheId, affKey, op, out -> {
            try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
                writeIdentity(w);

                w.writeBoolean(serverKeepBinary);
                w.writeInt(keys.size());

                w.writeObject(firstKey);

                while (iter.hasNext()) {
                    w.writeObject(iter.next());
                }
            }
        }, r -> r.in().readBoolean());
    }

    private <TR> TR op(ClientOperation op, Consumer<BinaryRawWriterEx> writer, Function<PayloadInputChannel, TR> reader) {
        return ch.service(op, out -> {
            try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
                writeIdentity(w);

                if (writer != null)
                    writer.accept(w);
            }
        }, reader);
    }

    private void writeIdentity(PayloadOutputChannel out) {
        try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
            writeIdentity(w);
        }
    }

    private void writeIdentity(BinaryRawWriterEx w) {
        w.writeString(name);
        w.writeInt(cacheId);
        w.writeBoolean(colocated);

        // TODO: Since ID is not used for affinity, we can drop it for simplicity and efficiency. Ignore same-name set issues.
        w.writeLong(id.globalId().getMostSignificantBits());
        w.writeLong(id.globalId().getLeastSignificantBits());
        w.writeLong(id.localId());
    }

    private Object affinityKey(Object key) {
        // CollocatedSetItemKey#setNameHash is AffinityKeyMapped.
        if (colocated)
            return name.hashCode();

        // Only separated mode is supported by the client partition awareness,
        // because older cluster nodes simply don't support this client feature.
        // Server wraps user object into GridCacheSetItemKey, but setId is always null in separated mode,
        // so the user object itself ends up as affinity key.
        return key;
    }

    private class SetIterator implements Iterator<T> {
        private final Long resourceId;

        private List<T> page;

        private int pos;

        public SetIterator(Long resourceId) {
            this.resourceId = resourceId;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            return null;
        }
    }
}
