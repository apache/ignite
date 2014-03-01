/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import java.util.*;

/**
 * Describes an index to be created for a certain type. It contains all necessary
 * information about fields, order, uniqueness, and specified
 * whether this is SQL or Text index.
 * See also {@link GridIndexingTypeDescriptor#indexes()}.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridIndexDescriptor {
    /**
     * Gets all fields to be indexed.
     *
     * @return Fields to be indexed.
     */
    public Collection<String> fields();

    /**
     * Specifies order of the index for each indexed field.
     *
     * @param field Field name.
     * @return {@code True} if given field should be indexed in descending order.
     */
    public boolean descending(String field);

    /**
     * Specifies whether this is a unique index.
     *
     * @return {@code True} if index is unique, {@code false} otherwise.
     */
    public boolean unique();

    /**
     * Specified if this is SQL or Text index.
     *
     * @return {@code True} if it is a text index, {@code false} for SQL index.
     */
    public boolean text();
}
