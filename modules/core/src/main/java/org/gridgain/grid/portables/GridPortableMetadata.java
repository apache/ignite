/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Portable type meta data. Metadata for portable types can be accessed from any of the
 * {@link org.apache.ignite.IgnitePortables#metadata(String) GridPortables.metadata(...)} methods.
 * Having metadata also allows for proper formatting of {@code GridPortableObject.toString()} method,
 * even when portable objects are kept in binary format only, which may be necessary for audit reasons.
 */
public interface GridPortableMetadata {
    /**
     * Gets portable type name.
     *
     * @return Portable type name.
     */
    public String typeName();

    /**
     * Gets collection of all field names for this portable type.
     *
     * @return Collection of all field names for this portable type.
     */
    public Collection<String> fields();

    /**
     * Gets name of the field type for a given field.
     *
     * @param fieldName Field name.
     * @return Field type name.
     */
    @Nullable public String fieldTypeName(String fieldName);

    /**
     * Portable objects can optionally specify custom key-affinity mapping in the
     * configuration. This method returns the name of the field which should be
     * used for the key-affinity mapping.
     *
     * @return Affinity key field name.
     */
    @Nullable public String affinityKeyFieldName();
}
