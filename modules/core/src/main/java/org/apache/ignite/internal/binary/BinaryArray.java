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
import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.UNREGISTERED_TYPE_ID;

/**
 * Binary object representing array.
 */
class BinaryArray implements BinaryObjectEx, Externalizable, Comparable<BinaryArray> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    @GridDirectTransient
    @GridToStringExclude
    protected BinaryContext ctx;

    /** Type ID. */
    protected int compTypeId;

    /** Type class name. */
    @Nullable protected String compClsName;

    /** Values. */
    @GridToStringInclude(sensitive = true)
    protected Object[] arr;

    /** Deserialized value. */
    @GridToStringExclude
    protected Object[] deserialized;

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

            // Skip deserialization if already deserialized.
            // Prepared result is in arr, already.
            if (deserialized != null)
                return (T)deserialized;

            deserialized = (Object[])Array.newInstance(compType, arr.length);

            for (int i = 0; i < arr.length; i++) {
                Object obj = CacheObjectUtils.unwrapBinaryIfNeeded(null, arr[i], false, false, ldr);

                if (obj instanceof BinaryObject)
                    obj = ((BinaryObject)obj).deserialize(ldr);

                deserialized[i] = obj;
            }

            return (T)deserialized;
        }
        finally {
            GridBinaryMarshaller.USE_CACHE.set(Boolean.TRUE);
        }
    }

    /** {@inheritDoc} */
    @Override public Object[] array() {
        return arr;
    }

    /** {@inheritDoc} */
    @Override public int componentTypeId() {
        // This can happen when binary type was not registered in time of binary array creation.
        // In this case same type will be written differently:
        // arr1 = [compTypeId=UNREGISTERED_TYPE_ID,compClsName="org.apache.Pojo"]
        // arr2 = [comTypeId=1234,compClsName=null]
        // Overcome by calculation compTypeId based on compClsName.
        return compTypeId == UNREGISTERED_TYPE_ID ? ctx.typeId(compClsName) : compTypeId;
    }

    /** {@inheritDoc} */
    @Override public String componentClassName() {
        return compClsName;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return new BinaryArray(ctx, compTypeId, compClsName, arr.clone());
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return GridBinaryMarshaller.OBJ_ARR;
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
        return 0;
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
    @Override public int hashCode() {
        int result = 31 * Objects.hash(componentTypeId());

        result = 31 * result + IgniteUtils.hashCode(arr);

        return result;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        BinaryArray arr = (BinaryArray)o;

        return componentTypeId() == arr.componentTypeId()
            && Arrays.deepEquals(this.arr, arr.arr);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull BinaryArray o) {
        if (componentTypeId() != o.componentTypeId()) {
            throw new IllegalArgumentException(
                "Can't compare arrays of different types[this=" + componentTypeId() + ",that=" + o.componentTypeId() + ']'
            );
        }

        return F.compareArrays(arr, o.arr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryArray.class, this);
    }
}
