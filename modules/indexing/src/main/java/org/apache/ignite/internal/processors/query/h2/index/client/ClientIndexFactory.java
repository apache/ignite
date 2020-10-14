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

        return new ClientInlineIndex(
            def.getSchema().getInlineKeys(),
            def.getCfgInlineSize(),
            def.getContext().config().getSqlIndexMaxInlineSize());
    }
}
