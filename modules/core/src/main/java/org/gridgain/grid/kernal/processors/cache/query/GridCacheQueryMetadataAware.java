/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.kernal.processors.query.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Metadata-aware interface.
 */
public interface GridCacheQueryMetadataAware {
    /**
     * @return Future to retrieve metadata.
     */
    public IgniteFuture<List<GridQueryFieldMetadata>> metadata();
}
