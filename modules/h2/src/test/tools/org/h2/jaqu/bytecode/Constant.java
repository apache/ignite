/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.bytecode;

import org.h2.jaqu.Token;

/**
 * An expression in the constant pool.
 */
public interface Constant extends Token {

    /**
     * The constant pool type.
     */
    enum Type {
        STRING,
        INT,
        FLOAT,
        DOUBLE,
        LONG,
        CLASS_REF,
        STRING_REF,
        FIELD_REF,
        METHOD_REF,
        INTERFACE_METHOD_REF,
        NAME_AND_TYPE
    }

    Constant.Type getType();

    int intValue();

}
