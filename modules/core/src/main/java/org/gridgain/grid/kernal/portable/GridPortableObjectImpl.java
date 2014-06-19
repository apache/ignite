/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.portable.GridPortableMarshaller.*;

/**
 * Portable object implementation.
 */
public class GridPortableObjectImpl implements GridPortableObject {
    /** */
    private final byte[] arr;

    /** */
    private final boolean isNull;

    /** */
    private final MetaData meta;

    /**
     * @param arr Byte array.
     * @throws GridPortableException In case of error.
     */
    public GridPortableObjectImpl(byte[] arr) throws GridPortableException {
        assert arr != null;

        if (arr.length == 0)
            throw new GridPortableException("Invalid portable object format.");

        this.arr = arr;

        isNull = arr[0] == NULL;

        meta = readMeta();
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return meta.typeId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String typeName() {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public Collection<String> fields() {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public int fieldTypeId(String fieldName) {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName(String fieldName) {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <F> F field(String fieldName) throws GridPortableException {
        if (isNull)
            return null;

        Integer off = null;

        if (meta.namedFields != null)
            off = meta.namedFields.get(fieldName);

        if (off == null)
            off = meta.hashedFields.get(fieldName.hashCode());

        if (off == null)
            return null;

        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends GridPortable> T deserialize() throws GridPortableException {
        if (isNull)
            return null;

        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableObject copy(@Nullable Map<String, Object> fields) {
        if (isNull)
            return null;

        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public GridPortableObject clone() {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        GridPortableObjectImpl obj = (GridPortableObjectImpl)other;

        return Arrays.equals(arr, obj.arr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return meta.hashCode;
    }

    /**
     * @return Meta data.
     */
    private MetaData readMeta() {
        return null;
    }

    /** */
    private static class MetaData {
        /** */
        private final int typeId;

        /** */
        private final int hashCode;

        /** */
        private final int len;

        /** */
        private final Map<Integer, Integer> hashedFields;

        /** */
        private final Map<String, Integer> namedFields;

        /**
         * @param typeId Type ID.
         * @param hashCode Hash code.
         * @param len Length.
         * @param hashedFields Hashed fields.
         * @param namedFields Named fields.
         */
        private MetaData(int typeId, int hashCode, int len, Map<Integer, Integer> hashedFields,
            @Nullable Map<String, Integer> namedFields) {
            assert hashedFields != null;

            this.typeId = typeId;
            this.hashCode = hashCode;
            this.len = len;
            this.hashedFields = hashedFields;
            this.namedFields = namedFields;
        }
    }
}
