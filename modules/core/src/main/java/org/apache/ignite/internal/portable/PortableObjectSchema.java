package org.apache.ignite.internal.portable;

import java.util.Map;

/**
 * Portable object schema.
 */
public class PortableObjectSchema {
    /** Schema ID. */
    private final int schemaId;

    /** Fields. */
    private final Map<Integer, Integer> fields;

    /**
     * Constructor.
     *
     * @param schemaId Schema ID.
     * @param fields Fields.
     */
    public PortableObjectSchema(int schemaId, Map<Integer, Integer> fields) {
        this.schemaId = schemaId;
        this.fields = fields;
    }

    /**
     * Get schema ID.
     *
     * @return Schema ID.
     */
    public int schemaId() {
        return schemaId;
    }

    /**
     * Get field offset position.
     *
     * @param fieldId Field ID.
     * @return Field offset position.
     */
    public int fieldOffsetPosition(int fieldId) {
        Integer pos = fields.get(fieldId);

        return pos != null ? pos : 0;
    }
}
