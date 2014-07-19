/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

/**
 * Defines portable objects functionality.
 */
public interface GridPortables {
    /**
     * Gets type ID for given type name.
     *
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName);

    /**
     * Converts provided object to instance of {@link GridPortableObject}.
     * <p>
     * Note that object's type needs to be configured in {@link GridPortableConfiguration}.
     *
     * @param obj Object to convert.
     * @return Converted object.
     */
    public <T> T toPortable(@Nullable Object obj) throws GridPortableException;

    /**
     * Gets portable builder.
     *
     * @return Portable builder.
     */
    public <T> GridPortableBuilder<T> builder();
}
