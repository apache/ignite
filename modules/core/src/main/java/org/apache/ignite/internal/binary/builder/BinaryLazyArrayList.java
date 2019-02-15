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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;

/**
 *
 */
class BinaryLazyArrayList extends AbstractList<Object> implements BinaryBuilderSerializationAware {
    /** */
    private final BinaryBuilderReader reader;

    /** */
    private final int off;

    /** */
    private List<Object> delegate;

    /**
     * @param reader Reader.
     * @param size Size,
     */
    BinaryLazyArrayList(BinaryBuilderReader reader, int size) {
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
            int size = reader.readIntPositioned(off + 1);

            reader.position(off + 1/* flag */ + 4/* size */ + 1/* col type */);

            delegate = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                delegate.add(reader.parseValue());
        }
    }

    /** {@inheritDoc} */
    @Override public Object get(int idx) {
        ensureDelegateInit();

        return BinaryUtils.unwrapLazy(delegate.get(idx));
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

        return BinaryUtils.unwrapLazy(delegate.set(idx, element));
    }

    /** {@inheritDoc} */
    @Override public Object remove(int idx) {
        ensureDelegateInit();

        return BinaryUtils.unwrapLazy(delegate.remove(idx));
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        if (delegate == null)
            delegate = new ArrayList<>();
        else
            delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(int idx, Collection<?> c) {
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
            return reader.readIntPositioned(off + 1);

        return delegate.size();
    }

    /** {@inheritDoc} */
    @Override public void writeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer ctx) {
        if (delegate == null) {
            int size = reader.readIntPositioned(off + 1);

            int hdrSize = 1 /* flag */ + 4 /* size */ + 1 /* col type */;

            writer.write(reader.array(), off, hdrSize);

            reader.position(off + hdrSize);

            for (int i = 0; i < size; i++) {
                Object o = reader.parseValue();

                ctx.writeValue(writer, o);
            }
        }
        else {
            writer.writeByte(GridBinaryMarshaller.COL);
            writer.writeInt(delegate.size());

            byte colType = reader.array()[off + 1 /* flag */ + 4 /* size */];
            writer.writeByte(colType);

            int oldPos = reader.position();

            for (Object o : delegate)
                ctx.writeValue(writer, o);

            // BinaryBuilderImpl might have been written. It could override reader's position.
            reader.position(oldPos);
        }
    }
}
