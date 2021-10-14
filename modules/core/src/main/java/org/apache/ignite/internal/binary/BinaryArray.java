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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Binary object representing array.
 */
public class BinaryArray implements BinaryObjectEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    @GridDirectTransient
    @GridToStringExclude
    private BinaryContext ctx;

    /** Type ID. */
    private int compTypeId;

    /** Type class name. */
    @Nullable private String compClsName;

    /** Values. */
    @GridToStringInclude
    private Object[] arr;

    /**
     * {@link Externalizable} support.
     */
    public BinaryArray() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param compTypeId Component type id.
     * @param compClsName Component class name.
     * @param arr Array.
     */
    public BinaryArray(BinaryContext ctx, int compTypeId, @Nullable String compClsName, Object[] arr) {
        this.ctx = ctx;
        this.compTypeId = compTypeId;
        this.compClsName = compClsName;
        this.arr = arr;
    }

    /** {@inheritDoc} */
    @Override public BinaryType type() throws BinaryObjectException {
        return BinaryUtils.typeProxy(ctx, this);
    }

    /** {@inheritDoc} */
    @Override public @Nullable BinaryType rawType() throws BinaryObjectException {
        return BinaryUtils.type(ctx, this);
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize() throws BinaryObjectException {
        return (T)deserialize(null);
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize(ClassLoader ldr) throws BinaryObjectException {
        ClassLoader resolveLdr = ldr == null ? ctx.configuration().getClassLoader() : ldr;

        if (ldr != null)
            GridBinaryMarshaller.USE_CACHE.set(Boolean.FALSE);

        try {
            Class<?> compType = BinaryUtils.resolveClass(ctx, compTypeId, compClsName, resolveLdr, false);

            Object[] res = (Object[])Array.newInstance(compType, arr.length);

            for (int i = 0; i < arr.length; i++)
                try {
                    Object obj = CacheObjectUtils.unwrapBinaryIfNeeded(null, arr[i], false, false, ldr);

                    if (BinaryObject.class.isAssignableFrom(obj.getClass()))
                        obj = ((BinaryObject)obj).deserialize(ldr);

                    res[i] = obj;
                }
                catch (ArrayStoreException e ) {
                    System.out.println("BinaryArray.deserialize");
                    throw e;
                }

            return (T)res;
        }
        finally {
            GridBinaryMarshaller.USE_CACHE.set(Boolean.TRUE);
        }
    }

    /**
     * @return Underlying array.
     */
    public Object[] array() {
        return arr;
    }

    /**
     * @return Component type ID.
     */
    public int componentTypeId() {
        return compTypeId;
    }

    /**
     * @return Component class name.
     */
    public String componentClassName() {
        return compClsName;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return new BinaryArray(ctx, compTypeId, compClsName, arr.clone());
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return GridBinaryMarshaller.BINARY_ARR;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(compTypeId);
        out.writeObject(compClsName);
        out.writeObject(arr);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = GridBinaryMarshaller.threadLocalContext();

        compTypeId = in.readInt();
        compClsName = (String)in.readObject();
        arr = (Object[])in.readObject();
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder toBuilder() throws BinaryObjectException {
        throw new UnsupportedOperationException("Builder cannot be created for array wrapper.");
    }

    /** {@inheritDoc} */
    @Override public int enumOrdinal() throws BinaryObjectException {
        throw new BinaryObjectException("Object is not enum.");
    }

    /** {@inheritDoc} */
    @Override public String enumName() throws BinaryObjectException {
        throw new BinaryObjectException("Object is not enum.");
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return -1; //TODO: fixme
    }

    /** {@inheritDoc} */
    @Override public boolean isFlagSet(short flag) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <F> F field(String fieldName) throws BinaryObjectException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryArray.class, this);
    }
}
