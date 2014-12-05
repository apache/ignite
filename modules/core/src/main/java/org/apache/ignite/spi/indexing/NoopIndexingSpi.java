/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.indexing;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation of {@link IndexingSpi}, throws exception on query attempt.
 */
@IgniteSpiNoop
public class NoopIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public <K, V> IndexingFieldsResult queryFields(@Nullable String spaceName, String qry,
        Collection<Object> params, IndexingQueryFilter filters) throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteSpiCloseableIterator<IndexingKeyValueRow<K, V>> query(@Nullable String spaceName,
        String qry, Collection<Object> params, IndexingTypeDescriptor type,
        IndexingQueryFilter filters) throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteSpiCloseableIterator<IndexingKeyValueRow<K, V>> queryText(@Nullable
        String spaceName, String qry, IndexingTypeDescriptor type, IndexingQueryFilter filters)
        throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String spaceName, IndexingTypeDescriptor desc) throws IgniteSpiException {
        throw spiException();
    }

    /** {@inheritDoc} */
    @Override public boolean registerType(@Nullable String spaceName, IndexingTypeDescriptor desc)
        throws IgniteSpiException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void unregisterType(@Nullable String spaceName, IndexingTypeDescriptor type)
        throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <K, V> void store(@Nullable String spaceName, IndexingTypeDescriptor type,
        IndexingEntity<K> key, IndexingEntity<V> val, byte[] ver, long expirationTime) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <K> boolean remove(@Nullable String spaceName, IndexingEntity<K> key) throws IgniteSpiException {
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
    @Override public void registerMarshaller(IndexingMarshaller marshaller) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void registerSpace(String spaceName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void rebuildIndexes(@Nullable String spaceName, IndexingTypeDescriptor type) {
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
