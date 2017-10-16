package org.apache.ignite.internal.processors.query.h2.database;

import org.h2.value.Value;

/**
 * Index value type.
 */
public class IndexValueType {
    public static final int NULL = Value.NULL;
    public static final int BOOLEAN = Value.BOOLEAN;
    public static final int BYTE = Value.BYTE;
    public static final int SHORT = Value.SHORT;
    public static final int INT = Value.INT;
    public static final int LONG = Value.LONG;
    public static final int FLOAT = Value.FLOAT;
    public static final int DOUBLE = Value.DOUBLE;
    public static final int DATE = Value.DATE;
    public static final int TIME = Value.TIME;
    public static final int TIMESTAMP = Value.TIMESTAMP;
    public static final int UUID = Value.UUID;
    public static final int STRING = Value.STRING;
    public static final int STRING_FIXED = Value.STRING_FIXED;
    public static final int STRING_IGNORECASE = Value.STRING_IGNORECASE;
    public static final int BYTES = Value.BYTES;
    public static final int UNKNOWN = Value.UNKNOWN;
}
