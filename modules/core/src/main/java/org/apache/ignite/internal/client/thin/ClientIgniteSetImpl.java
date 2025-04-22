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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.client.ClientAutoCloseableIterator;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Client Ignite Set.
 */
class ClientIgniteSetImpl<T> implements ClientIgniteSet<T> {
    /** */
    private final String name;

    /** */
    private final ReliableChannel ch;

    /** */
    private final ClientUtils serDes;

    /** */
    private final boolean colocated;

    /** */
    private final int cacheId;

    /** */
    private volatile boolean serverKeepBinary = true;

    /** */
    private volatile int pageSize = 1024;

    /**
     * Constructor.
     * @param ch Channel.
     * @param serDes Utils..
     * @param name Name.
     * @param colocated Colocated flag.
     * @param cacheId Cache id.
     */
    public ClientIgniteSetImpl(
            ReliableChannel ch,
            ClientUtils serDes,
            String name,
            boolean colocated,
            int cacheId) {
        assert ch != null;
        assert serDes != null;
        assert name != null;

        this.ch = ch;
        this.serDes = serDes;
        this.name = name;
        this.colocated = colocated;
        this.cacheId = cacheId;
    }

    /** {@inheritDoc} */
    @Override public boolean add(T o) {
        A.notNull(o, "o");

        return singleKeyOp(ClientOperation.OP_SET_VALUE_ADD, o);
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(Collection<? extends T> c) {
        A.notNull(c, "c");

        return multiKeyOp(ClientOperation.OP_SET_VALUE_ADD_ALL, c);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        op(ClientOperation.OP_SET_CLEAR, null, null);
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        A.notNull(o, "o");

        return singleKeyOp(ClientOperation.OP_SET_VALUE_CONTAINS, o);
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(Collection<?> c) {
        A.notNull(c, "c");

        return multiKeyOp(ClientOperation.OP_SET_VALUE_CONTAINS_ALL, c);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return size() == 0;
    }

    /** {@inheritDoc} */
    @Override public ClientAutoCloseableIterator<T> iterator() {
        Consumer<PayloadOutputChannel> payloadWriter = out -> {
            writeIdentity(out);
            out.out().writeInt(pageSize);
        };

        Function<PayloadInputChannel, ClientAutoCloseableIterator<T>> payloadReader = in -> {
            List<T> page = readPage(in);
            boolean hasNext = in.in().readBoolean();
            Long rsrcId = hasNext ? in.in().readLong() : null;
            ClientChannel rsrcCh = hasNext ? in.clientChannel() : null;

            return new PagedIterator(rsrcCh, rsrcId, page);
        };

        if (colocated) {
            Object affKey = name.hashCode();

            return ch.affinityService(cacheId, affKey, ClientOperation.OP_SET_ITERATOR_START, payloadWriter, payloadReader);
        }

        return ch.service(ClientOperation.OP_SET_ITERATOR_START, payloadWriter, payloadReader);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        A.notNull(o, "o");

        return singleKeyOp(ClientOperation.OP_SET_VALUE_REMOVE, o);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        A.notNull(c, "c");

        return multiKeyOp(ClientOperation.OP_SET_VALUE_REMOVE_ALL, c);
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> c) {
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

    /** {@inheritDoc} */
    @Override public int size() {
        return op(ClientOperation.OP_SET_SIZE, null, r -> r.in().readInt());
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        return toArray(X.EMPTY_OBJECT_ARRAY);
    }

    /** {@inheritDoc} */
    @Override public <T1> T1[] toArray(T1[] a) {
        try (ClientAutoCloseableIterator<T> it = iterator()) {
            ArrayList<T1> res = new ArrayList<>();

            while (it.hasNext())
                res.add((T1)it.next());

            return res.toArray(a);
        }
        catch (Exception e) {
            throw new IgniteClientException(ClientStatus.FAILED, e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        op(ClientOperation.OP_SET_CLOSE, null, null);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public boolean colocated() {
        return colocated;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return !op(ClientOperation.OP_SET_EXISTS, null, r -> r.in().readBoolean());
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

    /** {@inheritDoc} */
    @Override public ClientIgniteSet<T> pageSize(int pageSize) {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size must be greater than 0.");

        this.pageSize = pageSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return pageSize;
    }

    /**
     * Performs a single key operation.
     *
     * @param op Op code.
     * @param key Key.
     * @return Result.
     */
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

    /**
     * Performs a multi key operation.
     *
     * @param op Op code.
     * @param keys Keys.
     * @return Result.
     */
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

    /**
     * Performs an operation.
     *
     * @param op Op code.
     * @param writer Writer.
     * @param reader Reader.
     * @param <TR> Result type.
     * @return Result.
     */
    private <TR> TR op(ClientOperation op, Consumer<BinaryRawWriterEx> writer, Function<PayloadInputChannel, TR> reader) {
        return ch.service(op, out -> {
            try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
                writeIdentity(w);

                if (writer != null)
                    writer.accept(w);
            }
        }, reader);
    }

    /**
     * Writes identity.
     *
     * @param out Output channel.
     */
    private void writeIdentity(PayloadOutputChannel out) {
        try (BinaryRawWriterEx w = serDes.createBinaryWriter(out.out())) {
            writeIdentity(w);
        }
    }

    /**
     * Writes identity.
     *
     * @param w Writer.
     */
    private void writeIdentity(BinaryRawWriterEx w) {
        // IgniteSet is uniquely identified by name, cacheId, and colocated flag.
        // Just name and groupName are not enough, because target cache name depends on multiple config properties
        // (atomicity mode, backups, etc).
        // So cacheId replaces group name and all those properties. It also simplifies affinity calculation.
        w.writeString(name);
        w.writeInt(cacheId);
        w.writeBoolean(colocated);
    }

    /**
     * Gets affinity key for the user key.
     *
     * @param key Key.
     * @return Affinity key.
     */
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

    /**
     * Reads iterator page.
     *
     * @param in Input channel.
     * @return Page.
     */
    private List<T> readPage(PayloadInputChannel in) {
        try (BinaryRawReaderEx r = serDes.createBinaryReader(in.in())) {
            int size = r.readInt();
            List<T> res = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                res.add((T)r.readObject());

            return res;
        }
        catch (IOException e) {
            throw new ClientException(e);
        }
    }

    /**
     * Paged iterator.
     */
    private class PagedIterator implements ClientAutoCloseableIterator<T> {
        /** */
        private final ClientChannel resourceCh;

        /** */
        private Long resourceId;

        /** */
        private List<T> page;

        /** */
        private int pos;

        /**
         * Constructor.
         *
         * @param resourceCh Associated channel.
         * @param resourceId Resource id.
         * @param page First page.
         */
        public PagedIterator(ClientChannel resourceCh, Long resourceId, List<T> page) {
            assert page != null;
            assert (resourceCh == null) == (resourceId == null);

            this.resourceCh = resourceCh;
            this.resourceId = resourceId;
            this.page = page;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return pos < page.size();
        }

        /** {@inheritDoc} */
        @Override public T next() {
            if (!hasNext())
                throw new NoSuchElementException();

            T next = page.get(pos++);

            if (pos >= page.size() && resourceId != null)
                fetchNextPage();

            return next;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            Long id = resourceId;

            if (id == null)
                return;

            ch.service(ClientOperation.RESOURCE_CLOSE, w -> w.out().writeLong(id), null);

            resourceId = null;
            pos = Integer.MAX_VALUE;
        }

        /**
         * Fetches next page from the server.
         */
        private void fetchNextPage() {
            page = resourceCh.service(
                    ClientOperation.OP_SET_ITERATOR_GET_PAGE,
                    out -> {
                        out.out().writeLong(resourceId);
                        out.out().writeInt(pageSize);
                    },
                    in -> {
                        List<T> res = readPage(in);
                        boolean hasNext = in.in().readBoolean();

                        if (!hasNext)
                            resourceId = null;

                        return res;
                    });

            pos = 0;
        }
    }
}
