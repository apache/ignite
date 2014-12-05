/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation of {@link GridIndexingSpi}, throws exception on query attempt.
 */
@IgniteSpiNoop
public class GridNoopIndexingSpi extends IgniteSpiAdapter implements GridIndexingSpi {
    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public <K, V> GridIndexingFieldsResult queryFields(@Nullable String spaceName, String qry,
        Collection<Object> params, GridIndexingQueryFilter filters) throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteSpiCloseableIterator<GridIndexingKeyValueRow<K, V>> query(@Nullable String spaceName,
        String qry, Collection<Object> params, GridIndexingTypeDescriptor type,
        GridIndexingQueryFilter filters) throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteSpiCloseableIterator<GridIndexingKeyValueRow<K, V>> queryText(@Nullable
        String spaceName, String qry, GridIndexingTypeDescriptor type, GridIndexingQueryFilter filters)
        throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String spaceName, GridIndexingTypeDescriptor desc) throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public boolean registerType(@Nullable String spaceName, GridIndexingTypeDescriptor desc)
        throws IgniteSpiException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void unregisterType(@Nullable String spaceName, GridIndexingTypeDescriptor type)
        throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <K, V> void store(@Nullable String spaceName, GridIndexingTypeDescriptor type,
        GridIndexingEntity<K> key, GridIndexingEntity<V> val, byte[] ver, long expirationTime) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <K> boolean remove(@Nullable String spaceName, GridIndexingEntity<K> key) throws IgniteSpiException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <K> void onSwap(@Nullable String spaceName, String swapSpaceName, K key) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <K, V> void onUnswap(@Nullable String spaceName, K key, V val, byte[] valBytes)
        throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void registerMarshaller(GridIndexingMarshaller marshaller) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void registerSpace(String spaceName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void rebuildIndexes(@Nullable String spaceName, GridIndexingTypeDescriptor type) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /**
     * @return No-op SPI usage exception.
     */
    private IgniteSpiException spiException() {
        return new IgniteSpiException("Current grid configuration does not support queries " +
            "(please configure GridH2IndexingSpi).");
    }
}
