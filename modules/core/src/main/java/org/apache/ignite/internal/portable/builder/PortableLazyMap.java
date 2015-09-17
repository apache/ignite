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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class PortableLazyMap extends AbstractMap<Object, Object> implements PortableBuilderSerializationAware {
    /** */
    private final PortableBuilderReader reader;

    /** */
    private final int off;

    /** */
    private Map<Object, Object> delegate;

    /**
     * @param reader Reader.
     * @param off Offset.
     */
    private PortableLazyMap(PortableBuilderReader reader, int off) {
        this.reader = reader;
        this.off = off;
    }

    /**
     * @param reader Reader.
     * @return PortableLazyMap.
     */
    @Nullable public static PortableLazyMap parseMap(PortableBuilderReader reader) {
        int off = reader.position() - 1;

        int size = reader.readInt();

        reader.skip(1); // map type.

        for (int i = 0; i < size; i++) {
            reader.skipValue(); // skip key
            reader.skipValue(); // skip value
        }

        return new PortableLazyMap(reader, off);
    }

    /**
     *
     */
    private void ensureDelegateInit() {
        if (delegate == null) {
            int size = reader.readIntAbsolute(off + 1);

            reader.position(off + 1/* flag */ + 4/* size */ + 1/* col type */);

            delegate = new LinkedHashMap<>();

            for (int i = 0; i < size; i++)
                delegate.put(PortableUtils.unwrapLazy(reader.parseValue()), reader.parseValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
        if (delegate == null) {
            int size = reader.readIntAbsolute(off + 1);

            int hdrSize = 1 /* flag */ + 4 /* size */ + 1 /* col type */;
            writer.write(reader.array(), off, hdrSize);

            reader.position(off + hdrSize);

            for (int i = 0; i < size; i++) {
                ctx.writeValue(writer, reader.parseValue()); // key
                ctx.writeValue(writer, reader.parseValue()); // value
            }
        }
        else {
            writer.writeByte(GridPortableMarshaller.MAP);
            writer.writeInt(delegate.size());

            byte colType = reader.array()[off + 1 /* flag */ + 4 /* size */];

            writer.writeByte(colType);

            for (Entry<Object, Object> entry : delegate.entrySet()) {
                ctx.writeValue(writer, entry.getKey());
                ctx.writeValue(writer, entry.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        if (delegate == null)
            return reader.readIntAbsolute(off + 1);

        return delegate.size();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        ensureDelegateInit();

        return delegate.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(Object val) {
        return values().contains(val);
    }

    /** {@inheritDoc} */
    @Override public Set<Object> keySet() {
        ensureDelegateInit();

        return delegate.keySet();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        if (delegate == null)
            delegate = new LinkedHashMap<>();
        else
            delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public Object get(Object key) {
        ensureDelegateInit();

        return PortableUtils.unwrapLazy(delegate.get(key));
    }

    /** {@inheritDoc} */
    @Override public Object put(Object key, Object val) {
        ensureDelegateInit();

        return PortableUtils.unwrapLazy(delegate.put(key, val));
    }

    /** {@inheritDoc} */
    @Override public Object remove(Object key) {
        ensureDelegateInit();

        return PortableUtils.unwrapLazy(delegate.remove(key));
    }

    /** {@inheritDoc} */
    @Override public Set<Entry<Object, Object>> entrySet() {
        ensureDelegateInit();

        return new AbstractSet<Entry<Object, Object>>() {
            @Override public boolean contains(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override public Iterator<Entry<Object, Object>> iterator() {
                return new Iterator<Entry<Object, Object>>() {
                    /** */
                    private final Iterator<Entry<Object, Object>> itr = delegate.entrySet().iterator();

                    @Override public boolean hasNext() {
                        return itr.hasNext();
                    }

                    @Override public Entry<Object, Object> next() {
                        Entry<Object, Object> res = itr.next();

                        final Object val = res.getValue();

                        if (val instanceof PortableLazyValue) {
                            return new SimpleEntry<Object, Object>(res.getKey(), val) {
                                private static final long serialVersionUID = 0L;

                                @Override public Object getValue() {
                                    return ((PortableLazyValue)val).value();
                                }
                            };
                        }

                        return res;
                    }

                    @Override public void remove() {
                        itr.remove();
                    }
                };
            }

            @Override public int size() {
                return delegate.size();
            }
        };
    }
}
