package org.apache.ignite.internal.processors.query.h2.index.client;

import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexSchema;

/** */
public class ClientIndexDefinition implements IndexDefinition {
    /** */
    private final QueryIndexSchema schema;

    /** */
    private final int cfgInlineSize;

    /** */
    private final String idxName;

    /** */
    private final String cacheName;

    /** */
    public ClientIndexDefinition(String cacheName, String idxName, QueryIndexSchema schema, int cfgInlineSize) {
        this.cacheName = cacheName;
        this.idxName = idxName;
        this.schema = schema;
        this.cfgInlineSize = cfgInlineSize;
    }

    /** */
    public int getCfgInlineSize() {
        return cfgInlineSize;
    }

    /** */
    public QueryIndexSchema getSchema() {
        return schema;
    }

    /** For non-affinity node GridCacheContext is null. */
    @Override public GridCacheContext getContext() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String getIdxName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override public String getCacheName() {
        return cacheName;
    }
}
