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
public class GridPortableObjectImpl<T> implements GridPortableObject<T>, Externalizable {
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
     * @return Length.
     */
    public int length() {
        if (reader == null)
            reader = new GridPortableReaderImpl(ctx, arr, start);

        return reader.length();
    }

    /**
     * @return Detached portable object.
     */
    public GridPortableObject<T> detach() {
        if (detached())
            return this;

        int len = length();

        byte[] arr0 = new byte[len];

        U.arrayCopy(arr, start, arr0, 0, len);

        return new GridPortableObjectImpl<>(ctx, arr0, 0);
    }

    /**
     * @return Detached or not.
     */
    public boolean detached() {
        return start == 0 && length() == arr.length;
    }

    /**
     * @param ctx Context.
     */
    void context(GridPortableContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public boolean userType() {
        if (reader == null)
            reader = new GridPortableReaderImpl(ctx, arr, start);

        return reader.userType();
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        if (reader == null)
            reader = new GridPortableReaderImpl(ctx, arr, start);

        return reader.typeId();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <F> F field(String fieldName) throws GridPortableException {
        if (reader == null)
            reader = new GridPortableReaderImpl(ctx, arr, start);

        return (F)reader.unmarshal(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public T deserialize() throws GridPortableException {
        if (obj == null) {
            if (reader == null)
                reader = new GridPortableReaderImpl(ctx, arr, start);

            obj = reader.deserialize();
        }

        return (T)obj;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableObject<T> copy(@Nullable Map<String, Object> fields) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public GridPortableObject<T> clone() throws CloneNotSupportedException {
        return (GridPortableObject<T>)super.clone();
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

        for (int i = start, j = otherPo.start; i < start + len; i++, j++) {
            if (arr[i] != otherPo.arr[j])
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        if (reader == null)
            reader = new GridPortableReaderImpl(ctx, arr, start);

        return reader.objectHashCode();
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
