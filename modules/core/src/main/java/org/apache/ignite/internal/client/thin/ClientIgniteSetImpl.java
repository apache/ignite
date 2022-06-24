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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetItemKey;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Client Ignite Set.
 */
class ClientIgniteSetImpl<T> implements ClientIgniteSet<T> {
    /** */
    private final String name;

    /** */
    private final IgniteUuid id;

    /** */
    private final ReliableChannel ch;

    /** */
    private final ClientUtils serDes;

    /** */
    private final boolean collocated;

    /** */
    private volatile boolean removed;

    /**
     * Constructor.
     *  @param ch Channel.
     * @param serDes Utils..
     * @param name Name.
     * @param id Id.
     * @param collocated Colocated flag.
     */
    public ClientIgniteSetImpl(ReliableChannel ch, ClientUtils serDes, String name, IgniteUuid id, boolean collocated) {
        assert ch != null;
        assert serDes != null;
        assert name != null;
        assert id != null;

        this.ch = ch;
        this.serDes = serDes;
        this.name = name;
        this.id = id;
        this.collocated = collocated;
    }

    @Override
    public boolean add(T o) {
        A.notNull(o, "o");

        return singleKeyOp(ClientOperation.OP_SET_VALUE_ADD, o);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        A.notNull(c, "c");

        // TODO
        return false;
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

        // TODO
        return false;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<T> iterator() {
        // TODO: Should we use CacheWeakQueryIteratorsHolder here somehow to match thick API weak ref semantics?
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

        // TODO
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        A.notNull(c, "c");

        // TODO
        return false;
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
    public boolean collocated() {
        return collocated;
    }

    @Override
    public boolean removed() {
        if (removed)
            return true;

        removed = !op(ClientOperation.OP_SET_EXISTS, null, r -> r.in().readBoolean());

        return removed;
    }

    @Override
    public void affinityRun(IgniteRunnable job) {

    }

    @Override
    public <R> R affinityCall(IgniteCallable<R> job) {
        return null;
    }

    private Boolean singleKeyOp(ClientOperation op, Object key) {
        // TODO: Partition awareness - we need to know colocated flag.
        return ch.service(op, out -> {
            try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
                writeIdentity(w);
                w.writeObject(key);
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

    private void writeIdentity(BinaryRawWriterEx w) {
        w.writeString(name);

        // TODO: Since ID is not used for affinity, we can drop it for simplicity and efficiency. Ignore same-name set issues.
        w.writeLong(id.globalId().getMostSignificantBits());
        w.writeLong(id.globalId().getLeastSignificantBits());
        w.writeLong(id.localId());
    }

    private Object affinityKey(Object o) {
        // Only separated mode is supported by the client partition awareness,
        // because older cluster nodes simply don't support this client feature.
        if (collocated)
            return name.hashCode();

        // TODO: same code using thick client and thin client will end up with different hash for user objects,
        // because client passes objects in form of BinaryObject.
        // Write a test to confirm that we can put from thin and get from thick.
        return o;
    }
}
