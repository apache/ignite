/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.portable;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Portable type meta data.
 */
public interface GridPortableMetaData {
    /**
     * @return Type name.
     */
    public String typeName();

    /**
     * @return Field names.
     */
    public Collection<String> fields();

    /**
     * @param fieldName Field name.
     * @return Field type name.
     */
    @Nullable public String fieldTypeName(String fieldName);

    /**
     * @return Affinity key field name.
     */
    @Nullable public String affinityKeyFieldName();
}
