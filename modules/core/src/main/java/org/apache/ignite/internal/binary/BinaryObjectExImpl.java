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

package org.apache.ignite.internal.binary;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryIdentityResolver;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Internal binary object interface.
 */
public abstract class BinaryObjectExImpl implements BinaryObjectEx {
    /**
     * @return Length.
     */
    public abstract int length();

    /**
     * @return Object start.
     */
    public abstract int start();

    /**
     * @return {@code True} if object is array based.
     */
    public abstract boolean hasArray();

    /**
     * @return Object array if object is array based, otherwise {@code null}.
     */
    public abstract byte[] array();

    /**
     * @return Object offheap address is object is offheap based, otherwise 0.
     */
    public abstract long offheapAddress();

    /**
     * Gets field value.
     *
     * @param fieldId Field ID.
     * @return Field value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of any other error.
     */
    @Nullable public abstract <F> F field(int fieldId) throws BinaryObjectException;

    /** {@inheritDoc} */
    @Override public int enumOrdinal() throws BinaryObjectException {
        throw new BinaryObjectException("Object is not enum.");
    }

    /**
     * Get offset of data begin.
     *
     * @return Field value.
     */
    public abstract int dataStartOffset();

    /**
     * Get offset of the footer begin.
     *
     * @return Field value.
     */
    public abstract int footerStartOffset();

    /**
     * Get field by offset.
     *
     * @param order Field offset.
     * @return Field value.
     */
    @Nullable public abstract <F> F fieldByOrder(int order);

    /**
     * Create field comparer.
     *
     * @return Comparer.
     */
    public abstract BinarySerializedFieldComparator createFieldComparator();

    /**
     * @param ctx Reader context.
     * @param fieldName Field name.
     * @return Field value.
     */
    @Nullable protected abstract <F> F field(BinaryReaderHandles ctx, String fieldName);

    /**
     * @return {@code True} if object has schema.
     */
    public abstract boolean hasSchema();

    /**
     * Get schema ID.
     *
     * @return Schema ID.
     */
    public abstract int schemaId();

    /**
     * Create schema for object.
     *
     * @return Schema.
     */
    public abstract BinarySchema createSchema();

    /**
     * Get binary context.
     *
     * @return Binary context.
     */
    public abstract BinaryContext context();

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder toBuilder() throws BinaryObjectException {
        return BinaryObjectBuilderImpl.wrap(this);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return (BinaryObject)super.clone();
    }

    /** {@inheritDoc} */
    public boolean equals(Object other) {
        if (other == this)
            return true;

        if (!(other instanceof BinaryObject))
            return false;

        BinaryIdentityResolver identity = context().identity(typeId());

        if (identity == null)
            identity = BinaryArrayIdentityResolver.instance();

        return identity.equals(this, (BinaryObject)other);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        try {
            BinaryReaderHandles ctx = new BinaryReaderHandles();

            ctx.put(start(), this);

            return toString(ctx, new IdentityHashMap<BinaryObject, Integer>());
        }
        catch (BinaryObjectException e) {
            throw new IgniteException("Failed to create string representation of binary object.", e);
        }
    }

    /**
     * @param ctx Reader context.
     * @param handles Handles for already traversed objects.
     * @return String representation.
     */
    private String toString(BinaryReaderHandles ctx, IdentityHashMap<BinaryObject, Integer> handles) {
        int idHash = System.identityHashCode(this);
        int hash = hashCode();

        BinaryType meta;

        try {
            meta = rawType();
        }
        catch (BinaryObjectException ignore) {
            meta = null;
        }

        if (meta == null || !S.INCLUDE_SENSITIVE)
            return S.toString(S.INCLUDE_SENSITIVE ? BinaryObject.class.getSimpleName() : "BinaryObject",
                "idHash", idHash, false,
                "hash", hash, false,
                "typeId", typeId(), true);

        handles.put(this, idHash);

        SB buf = new SB(meta.typeName());

        if (meta.fieldNames() != null) {
            buf.a(" [idHash=").a(idHash).a(", hash=").a(hash);

            for (String name : meta.fieldNames()) {
                Object val = field(ctx, name);

                buf.a(", ").a(name).a('=');

                appendValue(val, buf, ctx, handles);
            }

            buf.a(']');
        }

        return buf.toString();
    }

    /**
     * @param val Value to append.
     * @param buf Buffer to append to.
     * @param ctx Reader context.
     * @param handles Handles for already traversed objects.
     */
    @SuppressWarnings("unchecked")
    private void appendValue(Object val, SB buf, BinaryReaderHandles ctx,
        IdentityHashMap<BinaryObject, Integer> handles) {
        if (val instanceof byte[])
            buf.a(Arrays.toString((byte[]) val));
        else if (val instanceof short[])
            buf.a(Arrays.toString((short[])val));
        else if (val instanceof int[])
            buf.a(Arrays.toString((int[])val));
        else if (val instanceof long[])
            buf.a(Arrays.toString((long[])val));
        else if (val instanceof float[])
            buf.a(Arrays.toString((float[])val));
        else if (val instanceof double[])
            buf.a(Arrays.toString((double[])val));
        else if (val instanceof char[])
            buf.a(Arrays.toString((char[])val));
        else if (val instanceof boolean[])
            buf.a(Arrays.toString((boolean[]) val));
        else if (val instanceof BigDecimal[])
            buf.a(Arrays.toString((BigDecimal[])val));
        else if (val instanceof IgniteUuid)
            buf.a(val);
        else if (val instanceof BinaryObjectExImpl) {
            BinaryObjectExImpl po = (BinaryObjectExImpl)val;

            Integer idHash0 = handles.get(val);

            if (idHash0 != null) {  // Circular reference.
                BinaryType meta0 = po.rawType();

                assert meta0 != null;

                buf.a(meta0.typeName()).a(" [hash=").a(idHash0).a(", ...]");
            }
            else
                buf.a(po.toString(ctx, handles));
        }
        else if (val instanceof Object[]) {
            Object[] arr = (Object[])val;

            buf.a('[');

            for (int i = 0; i < arr.length; i++) {
                Object o = arr[i];

                appendValue(o, buf, ctx, handles);

                if (i < arr.length - 1)
                    buf.a(", ");
            }
        }
        else if (val instanceof Iterable) {
            Iterable<Object> col = (Iterable<Object>)val;

            buf.a(col.getClass().getSimpleName()).a(" {");

            Iterator it = col.iterator();

            while (it.hasNext()) {
                Object o = it.next();

                appendValue(o, buf, ctx, handles);

                if (it.hasNext())
                    buf.a(", ");
            }

            buf.a('}');
        }
        else if (val instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>)val;

            buf.a(map.getClass().getSimpleName()).a(" {");

            Iterator<Map.Entry<Object, Object>> it = map.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<Object, Object> e = it.next();

                appendValue(e.getKey(), buf, ctx, handles);

                buf.a('=');

                appendValue(e.getValue(), buf, ctx, handles);

                if (it.hasNext())
                    buf.a(", ");
            }

            buf.a('}');
        }
        else
            buf.a(val);
    }
}
