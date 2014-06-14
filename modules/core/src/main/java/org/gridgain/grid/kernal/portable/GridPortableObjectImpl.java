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
}
