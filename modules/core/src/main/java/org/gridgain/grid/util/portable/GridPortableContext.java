/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.gridgain.grid.portable.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Portable context.
 */
public interface GridPortableContext extends Serializable {
    /**
     * Gets descriptor for class.
     *
     * @param cls Class.
     * @return Descriptor.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableClassDescriptor descriptorForClass(Class<?> cls) throws GridPortableException;

    /**
     * Gets descriptor for type ID.
     *
     * @param userType User type flag.
     * @param typeId Type ID.
     * @return Descriptor.
     */
    @Nullable public GridPortableClassDescriptor descriptorForTypeId(boolean userType, int typeId);

    /**
     * Gets collection type flag value.
     *
     * @param cls Collection class.
     * @return Collection type.
     */
    public byte collectionType(Class<? extends Collection> cls);

    /**
     * Gets map type flag value.
     *
     * @param cls Map class.
     * @return Map type.
     */
    public byte mapType(Class<? extends Map> cls);

    /**
     * Gets field ID.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName);
}
