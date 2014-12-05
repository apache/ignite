/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.indexing;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * Space name and key filter.
 */
public interface IndexingQueryFilter {
    /**
     * Creates optional predicate for space.
     *
     * @param spaceName Space name.
     * @return Predicate or {@code null} if no filtering is needed.
     */
    @Nullable public <K, V> IgniteBiPredicate<K, V> forSpace(String spaceName) throws GridException;
}
