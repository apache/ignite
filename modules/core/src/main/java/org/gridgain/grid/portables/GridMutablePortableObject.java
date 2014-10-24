/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

import org.jetbrains.annotations.*;

/**
 * TODO GG-9352 Fix java doc.
 */
public interface GridMutablePortableObject extends GridPortableWriter {
    /**
     * @param fldName Field name.
     * @return Value of the field.
     */
    <F> F fieldValue(String fldName);

    /**
     * Sets field value.
     *
     * @param fldName Field name.
     * @param val Field value.
     */
    void fieldValue(String fldName, @Nullable Object val);

    /**
     * @param hashCode Hash code to set.
     * @return this.
     */
    GridMutablePortableObject setHashCode(int hashCode);

    /**
     * @return Hashcode.
     */
    int getHashCode();

    /**
     * @return Serialized portable object.
     */
    GridPortableObject toPortableObject();
}
