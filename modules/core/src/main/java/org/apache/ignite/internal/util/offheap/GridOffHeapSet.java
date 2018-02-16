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

package org.apache.ignite.internal.util.offheap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.NotNull;

/**
 * Java {@code Set} implementation based on {@code GridOffHeapMap}.
 */
public class GridOffHeapSet<E> implements Set<E> {
    /** */
    private static final byte[] BYTE = {1};

    /** */
    private final ClassLoader clsLdr;

    /** */
    private final Marshaller marshaller;

    /** */
    private volatile GridOffHeapMap offHeapMap = GridOffHeapMapFactory.unsafeMap(0);

    /**
     * @param marshaller Marshaller that will be used for object serialization.
     * @param clsLdr That will use marshaller for object deserialization.
     */
    public GridOffHeapSet(Marshaller marshaller, ClassLoader clsLdr) {
        this.marshaller = marshaller;
        this.clsLdr = clsLdr;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return (int) offHeapMap.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object obj) {
        try {
            return offHeapMap.contains(obj.hashCode(), marshaller.marshal(obj));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<E> iterator() {
        return new BinaryIteratorAdapter();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T[] toArray(@NotNull T[] a) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean add(E key) {
        return offHeapMap.put(key.hashCode(), marshal(key), BYTE);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object obj) {
        return offHeapMap.removex(obj.hashCode(), marshal(obj));
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(@NotNull Collection<?> c) {
        for (Object obj : c) {
            if (!contains(obj))
                return false;
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(@NotNull Collection<? extends E> c) {
        for (E k : c) {
            if (!add(k))
                return false;
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        for (Object obj : c)
            remove(obj);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        GridOffHeapMap offHeapMap0 = offHeapMap;

        offHeapMap = GridOffHeapMapFactory.unsafeMap(0);

        offHeapMap0.destruct();
    }

    /**
     * Marshals object to byte array.
     *
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteException If marshalling failed.
     */
    private byte[] marshal(Object obj) {
        try {
            return marshaller.marshal(obj);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Unmarshal object from byte array using given class loader.
     *
     * @param arr Byte array.
     * @return Unmarshalled object.
     * @throws IgniteException If unmarshalling failed.
     */
    private E unmarshal(byte[] arr) {
        try {
            return marshaller.unmarshal(arr, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Iterator over off-heap map with deserialization.
     */
    private class BinaryIteratorAdapter implements Iterator<E> {

        /** */
        private final GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iter = offHeapMap.iterator();

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public E next() {
            return unmarshal(iter.next().getKey());
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            iter.remove();
        }
    }
}
