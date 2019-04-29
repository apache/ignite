/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * ENUM value.
 */
public class ValueEnum extends ValueEnumBase {

    private final ExtTypeInfoEnum enumerators;

    ValueEnum(ExtTypeInfoEnum enumerators, String label, int ordinal) {
        super(label, ordinal);
        this.enumerators = enumerators;
    }

    @Override
    public TypeInfo getType() {
        return enumerators.getType();
    }

    public ExtTypeInfoEnum getEnumerators() {
        return enumerators;
    }

}
