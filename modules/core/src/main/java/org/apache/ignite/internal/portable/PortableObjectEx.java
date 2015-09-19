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

package org.apache.ignite.internal.portable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.IdentityHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.portable.api.PortableException;
import org.apache.ignite.internal.portable.api.PortableMetadata;
import org.apache.ignite.internal.portable.api.PortableObject;
import org.jetbrains.annotations.Nullable;

/**
 * Internal portable object interface.
 */
public abstract class PortableObjectEx implements PortableObject {
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
     * @param ctx Reader context.
     * @param fieldName Field name.
     * @return Field name.
     */
    @Nullable protected abstract <F> F field(PortableReaderContext ctx, String fieldName);

    /** {@inheritDoc} */
    @Override public PortableObject clone() throws CloneNotSupportedException {
        return (PortableObject)super.clone();
    }

    /** {@inheritDoc} */
    public boolean equals(Object other) {
        if (other == this)
            return true;

        if (other == null)
            return false;

        if (!(other instanceof PortableObjectEx))
            return false;

        PortableObjectEx otherPo = (PortableObjectEx)other;

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
    private String toString(PortableReaderContext ctx, IdentityHashMap<PortableObject, Integer> handles) {
        int idHash = System.identityHashCode(this);

        PortableMetadata meta;

        try {
            meta = metaData();
        }
        catch (PortableException ignore) {
            meta = null;
        }

        if (meta == null)
            return "PortableObject [hash=" + idHash + ", typeId=" + typeId() + ']';

        handles.put(this, idHash);

        SB buf = new SB(meta.typeName());

        if (meta.fields() != null) {
            buf.a(" [hash=").a(idHash);

            for (String name : meta.fields()) {
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
                    if (val instanceof PortableObjectEx) {
                        PortableObjectEx po = (PortableObjectEx)val;

                        Integer idHash0 = handles.get(val);

                        if (idHash0 != null) {  // Circular reference.
                            PortableMetadata meta0 = po.metaData();

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
            PortableReaderContext ctx = new PortableReaderContext();

            ctx.setPortableHandler(start(), this);

            return toString(ctx, new IdentityHashMap<PortableObject, Integer>());
        }
        catch (PortableException e) {
            throw new IgniteException("Failed to create string representation of portable object.", e);
        }
    }
}