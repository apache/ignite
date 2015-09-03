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

package org.apache.ignite.internal.portable.builder;

import org.apache.ignite.internal.portable.*;

import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 *
 */
class PortableLazyLinkedList extends AbstractList<Object> implements PortableBuilderSerializationAware {
    /** */
    private final PortableBuilderReader reader;

    /** */
    private final int off;

    /** */
    private List<Object> delegate;

    /**
     * @param reader Reader.
     * @param size Size,
     */
    PortableLazyLinkedList(PortableBuilderReader reader, int size) {
        this.reader = reader;
        off = reader.position() - 1/* flag */ - 4/* size */ - 1/* col type */;

        assert size >= 0;

        for (int i = 0; i < size; i++)
            reader.skipValue();
    }

    /**
     *
     */
    private void ensureDelegateInit() {
        if (delegate == null) {
            int size = reader.readIntAbsolute(off + 1);

            reader.position(off + 1/* flag */ + 4/* size */ + 1/* col type */);

            delegate = new LinkedList<>();

            for (int i = 0; i < size; i++)
                delegate.add(reader.parseValue());
        }
    }

    /** {@inheritDoc} */
    @Override public Object get(int idx) {
        ensureDelegateInit();

        return PortableUtils.unwrapLazy(delegate.get(idx));
    }

    /** {@inheritDoc} */
    @Override public boolean add(Object o) {
        ensureDelegateInit();

        return delegate.add(o);
    }

    /** {@inheritDoc} */
    @Override public void add(int idx, Object element) {
        ensureDelegateInit();

        delegate.add(idx, element);
    }

    /** {@inheritDoc} */
    @Override public Object set(int idx, Object element) {
        ensureDelegateInit();

        return PortableUtils.unwrapLazy(delegate.set(idx, element));
    }

    /** {@inheritDoc} */
    @Override public Object remove(int idx) {
        ensureDelegateInit();

        return PortableUtils.unwrapLazy(delegate.remove(idx));
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        if (delegate == null)
            delegate = new LinkedList<>();
        else
            delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(int idx, Collection<?> c) {
        ensureDelegateInit();

        return delegate.addAll(idx, c);
    }

    /** {@inheritDoc} */
    @Override protected void removeRange(int fromIdx, int toIdx) {
        ensureDelegateInit();

        delegate.subList(fromIdx, toIdx).clear();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        if (delegate == null)
            return reader.readIntAbsolute(off + 1);

        return delegate.size();
    }

    /** {@inheritDoc} */
    @Override public ListIterator<Object> listIterator(final int idx) {
        ensureDelegateInit();

        return new ListIterator<Object>() {
            /** */
            private final ListIterator<Object> delegate = PortableLazyLinkedList.super.listIterator(idx);

            @Override public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override public Object next() {
                return PortableUtils.unwrapLazy(delegate.next());
            }

            @Override public boolean hasPrevious() {
                return delegate.hasPrevious();
            }

            @Override public Object previous() {
                return PortableUtils.unwrapLazy(delegate.previous());
            }

            @Override public int nextIndex() {
                return delegate.nextIndex();
            }

            @Override public int previousIndex() {
                return delegate.previousIndex();
            }

            @Override public void remove() {
                delegate.remove();
            }

            @Override public void set(Object o) {
                delegate.set(o);
            }

            @Override public void add(Object o) {
                delegate.add(o);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterator<Object> iterator() {
        ensureDelegateInit();

        return PortableUtils.unwrapLazyIterator(super.iterator());
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        if (delegate == null) {
            int size = reader.readIntAbsolute(off + 1);

            int hdrSize = 1 /* flag */ + 4 /* size */ + 1 /* col type */;
            writer.write(reader.array(), off, hdrSize);

            reader.position(off + hdrSize);

            for (int i = 0; i < size; i++) {
                Object o = reader.parseValue();

                ctx.writeValue(writer, o);
            }
        }
        else {
            writer.writeByte(GridPortableMarshaller.COL);
            writer.writeInt(delegate.size());

            byte colType = reader.array()[off + 1 /* flag */ + 4 /* size */];
            writer.writeByte(colType);

            for (Object o : delegate)
                ctx.writeValue(writer, o);
        }
    }
}
