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

/**
 * Portable object implementation.
 */
class GridPortableObjectImpl implements GridPortableObject {
    /** */
    private final GridPortableReader reader;

    /** */
    private final boolean userType;

    /** */
    private final int typeId;

    /** */
    private final int hashCode;

    /**
     * @param reader Reader.
     * @param userType User type flag.
     * @param typeId Type ID.
     * @param hashCode Hash code.
     */
    GridPortableObjectImpl(GridPortableReader reader, boolean userType, int typeId, int hashCode) {
        assert reader != null;

        this.reader = reader;
        this.userType = userType;
        this.typeId = typeId;
        this.hashCode = hashCode;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public int fieldTypeId(String fieldName) {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <F> F field(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends GridPortable> T deserialize() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableObject copy(@Nullable Map<String, Object> fields) {
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

        return false;// Arrays.equals(arr, obj.arr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return hashCode;
    }
}
