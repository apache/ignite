/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.indexing;

import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Default implementation of {@link GridIndexingSpi} which does not index cache.
 */
@IgniteSpiNoop
public class GridNoopIndexingSpi extends IgniteSpiAdapter implements GridIndexingSpi {
    /** {@inheritDoc} */
    @Override public Iterator<?> query(@Nullable String spaceName, Collection<Object> params,
        @Nullable GridIndexingQueryFilter filters) throws IgniteSpiException {
        throw new IgniteSpiException("You have to configure custom GridIndexingSpi implementation.");
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String spaceName, Object key, Object val, long expirationTime)
        throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, Object key) throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void onSwap(@Nullable String spaceName, Object key) throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void onUnswap(@Nullable String spaceName, Object key, Object val) throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }
}
