package org.apache.ignite.internal.processors.query.calcite.exec.ddl;

import java.util.function.Supplier;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQuerySchemaManager;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;

/**
 * Schema manager.
 */
class SchemaManager implements GridQuerySchemaManager {
    /** Schema holder. */
    private final Supplier<SchemaPlus> schemaSupp;

    /**
     * @param schemaSupp Schema supplier.
     */
    SchemaManager(Supplier<SchemaPlus> schemaSupp) {
        this.schemaSupp = schemaSupp;
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescriptorForTable(String schemaName, String tableName) {
        SchemaPlus schema = schemaSupp.get().getSubSchema(schemaName);

        if (schema == null)
            return null;

        IgniteTable tbl = (IgniteTable)schema.getTable(tableName);

        return tbl == null ? null : tbl.descriptor().typeDescription();
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescriptorForIndex(String schemaName, String idxName) {
        SchemaPlus schema = schemaSupp.get().getSubSchema(schemaName);

        if (schema == null)
            return null;

        for (String tableName : schema.getTableNames()) {
            Table tbl = schema.getTable(tableName);

            if (tbl instanceof IgniteTable && ((IgniteTable)tbl).getIndex(idxName) != null)
                return ((IgniteTable)tbl).descriptor().typeDescription();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheContextInfo<K, V> cacheInfoForTable(String schemaName, String tableName) {
        SchemaPlus schema = schemaSupp.get().getSubSchema(schemaName);

        if (schema == null)
            return null;

        IgniteTable tbl = (IgniteTable)schema.getTable(tableName);

        return tbl == null ? null : (GridCacheContextInfo<K, V>)tbl.descriptor().cacheInfo();
    }
}
