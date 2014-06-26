/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Portable object implementation.
 */
public class GridPortableObjectImpl implements GridPortableObject, Externalizable {
    /** */
    private static final GridPortablePrimitives PRIM = GridPortablePrimitives.get();

    /** */
    private GridPortableContext ctx;

    /** */
    private byte[] arr;

    /** */
    private int start;

    /** */
    private transient GridPortableReaderImpl reader;

    /** */
    private transient Object obj;

    /** */
    private transient Map<String, Object> fields;

    /**
     * For {@link Externalizable}.
     */
    public GridPortableObjectImpl() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param arr Array.
     * @param start Start.
     */
    public GridPortableObjectImpl(GridPortableContext ctx, byte[] arr, int start) {
        assert ctx != null;
        assert arr != null;

        this.ctx = ctx;
        this.arr = arr;
        this.start = start;
    }

    /**
     * @param ctx Context.
     */
    void context(GridPortableContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @return Length.
     */
    int length() {
        return PRIM.readInt(arr, start + 10);
    }

    /** {@inheritDoc} */
    @Override public boolean userType() {
        return PRIM.readBoolean(arr, start + 1);
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return PRIM.readInt(arr, start + 2);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <F> F field(String fieldName) throws GridPortableException {
        if (fields != null) {
            if (fields.containsKey(fieldName))
                return (F)fields.get(fieldName);
        }
        else
            fields = new HashMap<>();

        if (reader == null)
            reader = new GridPortableReaderImpl(ctx, arr, start);

        Object field = reader.unmarshal(fieldName);

        fields.put(fieldName, field);

        return (F)field;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T deserialize() throws GridPortableException {
        if (obj == null) {
            if (reader == null)
                reader = new GridPortableReaderImpl(ctx, arr, start);

            obj = reader.readObject();
        }

        return (T)obj;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableObject copy(@Nullable Map<String, Object> fields) {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public GridPortableObject clone() throws CloneNotSupportedException {
        return (GridPortableObject)super.clone();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        GridPortableObjectImpl otherPo = (GridPortableObjectImpl)other;

        int len = length();
        int otherLen = otherPo.length();

        if (len != otherLen)
            return false;

        for (int i = start, j = otherPo.start; i < len; i++, j++) {
            if (arr[i] != otherPo.arr[j])
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return PRIM.readInt(arr, start + 6);
    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws GridPortableException {
        GridPortableRawWriter raw = writer.rawWriter();

        raw.writeByteArray(arr);
        raw.writeInt(start);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws GridPortableException {
        GridPortableRawReader raw = reader.rawReader();

        arr = raw.readByteArray();
        start = raw.readInt();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
        U.writeByteArray(out, arr);
        out.writeInt(start);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridPortableContext)in.readObject();
        arr = U.readByteArray(in);
        start = in.readInt();
    }
}
