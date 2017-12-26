package org.apache.ignite.ml.structures;

public class FeatureMetadata {
    /** Feature name */
    private String name;

    /** Java type to keep metadata */
    private Class type = String.class;

    public FeatureMetadata(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class type() {
        return type;
    }

    public void setType(Class type) {
        this.type = type;
    }
}
