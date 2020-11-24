package org.apache.ignite.internal.processors.query.h2.index.client;

import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.cache.query.index.IndexFactory;

/**
 * Factory fot client index.
 */
public class ClientIndexFactory implements IndexFactory {
    /** {@inheritDoc} */
    @Override public Index createIndex(IndexDefinition definition) {
        ClientIndexDefinition def = (ClientIndexDefinition) definition;

        int maxInlineSize = def.getContext() != null ? def.getContext().config().getSqlIndexMaxInlineSize() : -1;

        return new ClientInlineIndex(
            def.getIdxName(),
            def.getSchema().getInlineKeys(),
            def.getCfgInlineSize(),
            maxInlineSize);
    }
}
