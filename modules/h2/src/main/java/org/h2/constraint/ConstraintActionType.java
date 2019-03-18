/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constraint;

public enum ConstraintActionType {
    /**
     * The action is to restrict the operation.
     */
    RESTRICT,

    /**
     * The action is to cascade the operation.
     */
    CASCADE,

    /**
     * The action is to set the value to the default value.
     */
    SET_DEFAULT,

    /**
     * The action is to set the value to NULL.
     */
    SET_NULL;

    /**
     * Get standard SQL type name.
     *
     * @return standard SQL type name
     */
    public String getSqlName() {
        if (this == ConstraintActionType.SET_DEFAULT) {
            return "SET DEFAULT";
        }
        if (this == SET_NULL) {
            return "SET NULL";
        }
        return name();
    }

}