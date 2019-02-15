/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.binary.builder;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class BinaryLazyMap extends AbstractMap<Object, Object> implements BinaryBuilderSerializationAware {
    /** */
    private final BinaryBuilderReader reader;

    /** */
    private final int off;

    /** */
    private Map<Object, Object> delegate;

    /**
     * @param reader Reader.
     * @param off Offset.
     */
    private BinaryLazyMap(BinaryBuilderReader reader, int off) {
        this.reader = reader;
        this.off = off;
    }

    /**
     * @param reader Reader.
     * @return BinaryLazyMap.
     */
    @Nullable public static BinaryLazyMap parseMap(BinaryBuilderReader reader) {
        int off = reader.position() - 1;

        int size = reader.readInt();

        reader.skip(1); // map type.

        for (int i = 0; i < size; i++) {
            reader.skipValue(); // skip key
            reader.skipValue(); // skip value
        }

        return new BinaryLazyMap(reader, off);
    }

    /**
     *
     */
    private void ensureDelegateInit() {
        if (delegate == null) {
            int size = reader.readIntPositioned(off + 1);

            reader.position(off + 1/* flag */ + 4/* size */ + 1/* col type */);

            delegate = new LinkedHashMap<>();

            for (int i = 0; i < size; i++)
                delegate.put(BinaryUtils.unwrapLazy(reader.parseValue()), reader.parseValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer ctx) {
        if (delegate == null) {
            int size = reader.readIntPositioned(off + 1);

            int hdrSize = 1 /* flag */ + 4 /* size */ + 1 /* col type */;
            writer.write(reader.array(), off, hdrSize);

            reader.position(off + hdrSize);

            for (int i = 0; i < size; i++) {
                ctx.writeValue(writer, reader.parseValue()); // key
                ctx.writeValue(writer, reader.parseValue()); // value
            }
        }
        else {
            writer.writeByte(GridBinaryMarshaller.MAP);
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
            return reader.readIntPositioned(off + 1);

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

        return BinaryUtils.unwrapLazy(delegate.get(key));
    }

    /** {@inheritDoc} */
    @Override public Object put(Object key, Object val) {
        ensureDelegateInit();

        return BinaryUtils.unwrapLazy(delegate.put(key, val));
    }

    /** {@inheritDoc} */
    @Override public Object remove(Object key) {
        ensureDelegateInit();

        return BinaryUtils.unwrapLazy(delegate.remove(key));
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

                        if (val instanceof BinaryLazyValue) {
                            return new SimpleEntry<Object, Object>(res.getKey(), val) {
                                private static final long serialVersionUID = 0L;

                                @Override public Object getValue() {
                                    return ((BinaryLazyValue)val).value();
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
