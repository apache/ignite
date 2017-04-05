package org.apache.ignite.internal.processors.query.index.operation;

import java.util.UUID;

/**
 * Schema index abstract operation.
 */
public abstract class SchemaIndexAbstractOperation extends SchemaAbstractOperation {
    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param space Space.
     */
    public SchemaIndexAbstractOperation(UUID opId, String space) {
        super(opId, space);
    }

    /**
     * @return Index name.
     */
    public abstract String indexName();
}
