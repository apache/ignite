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
public class GridPortableObjectImpl implements GridPortableObject {
    /** */
    private final byte[] arr;



    /**
     * @param arr Byte array.
     */
    public GridPortableObjectImpl(byte[] arr) {
        assert arr != null;

        this.arr = arr;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return 0; // TODO: implement.
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
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public <T extends GridPortable> T deserialize() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public GridPortableObject copy(@Nullable Map<String, Object> fields) {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GridPortableObjectImpl object = (GridPortableObjectImpl)o;

        if (!Arrays.equals(arr, object.arr)) return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 0;
    }
}
