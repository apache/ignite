package org.apache.ignite.internal.processors.query.schema.operation;

import java.util.UUID;

/**
 * Parent class for ALTER TABLE command variants.
 */
public class SchemaAbstractAlterTableOperation extends SchemaAbstractOperation {
    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     */
    public SchemaAbstractAlterTableOperation(UUID opId, String cacheName, String schemaName) {
        super(opId, cacheName, schemaName);
    }
}
