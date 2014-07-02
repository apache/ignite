/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.portable;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Wrapper for serialized portable object.
 */
public interface GridPortableObject<T> extends Serializable, Cloneable {
    /**
     * Gets user type flag value.
     *
     * @return Whether this is a user type object.
     */
    public boolean userType();

    /**
     * Gets portable object type ID.
     *
     * @return Type ID.
     */
    public int typeId();

    /**
     * Gets field value.
     *
     * @param fieldName Field name.
     * @return Field value.
     * @throws GridPortableException In case of any other error.
     */
    @Nullable public <F> F field(String fieldName) throws GridPortableException;

    /**
     * Gets fully deserialized instance of portable object.
     *
     * @return Fully deserialized instance of portable object.
     * @throws GridPortableInvalidClassException If class doesn't exist.
     * @throws GridPortableException In case of any other error.
     */
    @Nullable public T deserialize() throws GridPortableException;

    /**
     * Creates a copy of this portable object and optionally changes field values
     * if they are provided in map. If map is empty or {@code null}, clean copy
     * is created.
     *
     * @param fields Fields to modify in copy.
     * @return Copy of this portable object.
     */
    public GridPortableObject<T> copy(@Nullable Map<String, Object> fields);

    /**
     * Copies this portable object.
     *
     * @return Copy of this portable object.
     */
    public GridPortableObject<T> clone() throws CloneNotSupportedException;
}
