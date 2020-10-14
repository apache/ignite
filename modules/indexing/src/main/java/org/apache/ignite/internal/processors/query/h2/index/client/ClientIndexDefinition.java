package org.apache.ignite.internal.processors.query.h2.index.client;

import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexSchema;

/** */
public class ClientIndexDefinition implements IndexDefinition {
    /** */
    private final QueryIndexSchema schema;

    /** */
    private final int cfgInlineSize;

    /** */
    private final GridCacheContext ctx;

    /** */
    private final String idxName;

    /** */
    public ClientIndexDefinition(GridCacheContext ctx, String idxName,
        QueryIndexSchema schema, int cfgInlineSize) {
        this.schema = schema;
        this.ctx = ctx;
        this.idxName = idxName;
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

    /** {@inheritDoc} */
    @Override public GridCacheContext getContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public String getIdxName() {
        return idxName;
    }
}
