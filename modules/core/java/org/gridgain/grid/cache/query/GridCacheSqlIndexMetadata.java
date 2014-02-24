// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.cache.query.*;

import java.io.*;
import java.util.*;

/**
 * GridGain index descriptor.
 * <p>
 * Provides information about one of the indexes created by
 * {@link org.gridgain.grid.spi.indexing.GridIndexingSpi}.
 * <p>
 * All index descriptors can be obtained from
 * {@link GridCacheSqlMetadata#indexes(String)} method.
 *
 * @author @java.author
 * @version @java.version
 * @see GridCacheSqlMetadata
 */
public interface GridCacheSqlIndexMetadata extends Externalizable {
    /**
     * Gets name of the index.
     *
     * @return Index name.
     */
    public String name();

    /**
     * Gets names of fields indexed by this index.
     *
     * @return Indexed fields names.
     */
    public Collection<String> fields();

    /**
     * Gets order of the index for each indexed field.
     *
     * @param field Field name.
     * @return {@code True} if given field is indexed in descending order.
     */
    public boolean descending(String field);

    /**
     * Gets whether this is a unique index.
     *
     * @return {@code True} if index is unique, {@code false} otherwise.
     */
    public boolean unique();
}
