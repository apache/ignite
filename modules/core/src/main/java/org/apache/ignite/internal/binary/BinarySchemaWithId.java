package org.apache.ignite.internal.binary;

import java.io.Serializable;

public class BinarySchemaWithId implements Serializable {

    private static final long serialVersionUID = 0L;

    private int schemaId;
    private BinarySchema schema;

    BinarySchemaWithId(int schemaId, BinarySchema schema) {
        this.schemaId = schemaId;
        this.schema = schema;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public BinarySchema getSchema() {
        return schema;
    }
}