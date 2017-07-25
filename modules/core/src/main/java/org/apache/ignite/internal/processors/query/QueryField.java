package org.apache.ignite.internal.processors.query;

import java.io.Serializable;

/**
 * Query field metadata.
 */
public class QueryField implements Serializable {
    /** Field name. */
    private String name;

    /** Class name for this field's values. */
    private String typeName;

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     */
    public QueryField(String name, String typeName) {
        this.name = name;
        this.typeName = typeName;
    }

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

    public String typeName() {
        return typeName;
    }

    public void typeName(String typeName) {
        this.typeName = typeName;
    }
}
