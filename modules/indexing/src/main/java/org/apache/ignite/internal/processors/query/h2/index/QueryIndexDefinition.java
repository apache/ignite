package org.apache.ignite.internal.processors.query.h2.index;

import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;

/**
 * Define H2 query index.
 */
public class QueryIndexDefinition extends SortedIndexDefinition {
    /** H2 query index schema. */
    private final QueryIndexSchema schema;

    /** */
    public QueryIndexDefinition(GridCacheContext ctx, String idxName, int segments,
        QueryIndexSchema schema, int cfgInlineSize) {
        super(ctx, idxName, segments, schema, new H2RowComparator(schema.table), cfgInlineSize);

        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override public String getTreeName() {
        GridQueryTypeDescriptor typeDesc = schema.table.rowDescriptor().type();

        int typeId = getContext().binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        // TODO: H2Tree in treeName, can change it?
        return BPlusTree.treeName((schema.table.rowDescriptor() == null ? "" : typeId + "_") + getIdxName(),
            "H2Tree");
    }
}
