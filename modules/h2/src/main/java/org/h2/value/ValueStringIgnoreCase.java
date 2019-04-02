/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.SysProperties;
import org.h2.util.StringUtils;

/**
 * Implementation of the VARCHAR_IGNORECASE data type.
 */
public class ValueStringIgnoreCase extends ValueString {

    private static final ValueStringIgnoreCase EMPTY =
            new ValueStringIgnoreCase("");
    private int hash;

    protected ValueStringIgnoreCase(String value) {
        super(value);
    }

    @Override
    public int getValueType() {
        return STRING_IGNORECASE;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode) {
        return mode.compareString(value, ((ValueStringIgnoreCase) o).value, true);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueString
                && value.equalsIgnoreCase(((ValueString) other).value);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            // this is locale sensitive
            hash = value.toUpperCase().hashCode();
        }
        return hash;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append("CAST(");
        return StringUtils.quoteStringSQL(builder, value).append(" AS VARCHAR_IGNORECASE)");
    }

    /**
     * Get or create a case insensitive string value for the given string.
     * The value will have the same case as the passed string.
     *
     * @param s the string
     * @return the value
     */
    public static ValueStringIgnoreCase get(String s) {
        int length = s.length();
        if (length == 0) {
            return EMPTY;
        }
        ValueStringIgnoreCase obj = new ValueStringIgnoreCase(StringUtils.cache(s));
        if (length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        ValueStringIgnoreCase cache = (ValueStringIgnoreCase) Value.cache(obj);
        // the cached object could have the wrong case
        // (it would still be 'equal', but we don't like to store it)
        if (cache.value.equals(s)) {
            return cache;
        }
        return obj;
    }

    @Override
    protected ValueString getNew(String s) {
        return ValueStringIgnoreCase.get(s);
    }

}
