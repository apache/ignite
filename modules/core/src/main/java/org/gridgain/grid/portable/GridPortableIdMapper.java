/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import org.jetbrains.annotations.*;

/**
 * Field mapper.
 */
public interface GridPortableIdMapper {
    /**
     * Gets type ID for provided type name.
     * <p>
     * If {@code null} is returned, hash code of type name will be used.
     *
     * @param typeName Type name.
     * @return Type ID.
     */
    @Nullable public Integer typeId(String typeName);

    /**
     * Gets type ID for provided field.
     * <p>
     * If {@code null} is returned, hash code of field name will be used.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    @Nullable public Integer fieldId(int typeId, String fieldName);
}
