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

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof BinarySchemaWithId)) {
            return false;
        }

        BinarySchemaWithId bswi = (BinarySchemaWithId) o;
        return this.schemaId == bswi.schemaId && this.schema.equals(bswi.schema);
    }

}