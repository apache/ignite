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
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryObject;
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
    protected abstract boolean hasArray();

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
     * Get field by offset.
     *
     * @param fieldOffset Field offset.
     * @return Field value.
     */
    @Nullable protected abstract <F> F fieldByOrder(int fieldOffset);

    /**
     * @param ctx Reader context.
     * @param fieldName Field name.
     * @return Field value.
     */
    @Nullable protected abstract <F> F field(BinaryReaderHandles ctx, String fieldName);

    /**
     * Get schema ID.
     *
     * @return Schema ID.
     */
    protected abstract int schemaId();

    /**
     * Create schema for object.
     *
     * @return Schema.
     */
    protected abstract BinarySchema createSchema();

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

        if (other == null)
            return false;

        if (!(other instanceof BinaryObjectExImpl))
            return false;

        BinaryObjectExImpl otherPo = (BinaryObjectExImpl)other;

        if (length() != otherPo.length() || typeId() != otherPo.typeId())
            return false;

        if (hasArray()) {
            if (otherPo.hasArray()) {
                int len = length();
                int end = start() + len;

                byte[] arr = array();
                byte[] otherArr = otherPo.array();

                for (int i = start(), j = otherPo.start(); i < end; i++, j++) {
                    if (arr[i] != otherArr[j])
                        return false;
                }

                return true;
            }
            else {
                assert otherPo.offheapAddress() > 0;

                return GridUnsafeMemory.compare(otherPo.offheapAddress() + otherPo.start(), array());
            }
        }
        else {
            assert offheapAddress() > 0;

            if (otherPo.hasArray())
                return GridUnsafeMemory.compare(offheapAddress() + start(), otherPo.array());
            else {
                assert otherPo.offheapAddress() > 0;

                return GridUnsafeMemory.compare(offheapAddress() + start(),
                    otherPo.offheapAddress() + otherPo.start(),
                    length());
            }
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

        if (meta == null)
            return BinaryObject.class.getSimpleName() +  " [idHash=" + idHash + ", hash=" + hash + ", typeId=" + typeId() + ']';

        handles.put(this, idHash);

        SB buf = new SB(meta.typeName());

        if (meta.fieldNames() != null) {
            buf.a(" [idHash=").a(idHash).a(", hash=").a(hash);

            for (String name : meta.fieldNames()) {
                Object val = field(ctx, name);

                buf.a(", ").a(name).a('=');

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
                else {
                    if (val instanceof BinaryObjectExImpl) {
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
                    else
                        buf.a(val);
                }
            }

            buf.a(']');
        }

        return buf.toString();
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
}
