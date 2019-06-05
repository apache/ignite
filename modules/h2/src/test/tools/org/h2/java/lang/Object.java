/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

/**
 * A java.lang.Object implementation.
 */
public class Object {

    @Override
    public int hashCode() {
        return 0;
    }

    public boolean equals(Object other) {
        return other == this;
    }

    @Override
    public java.lang.String toString() {
        return "?";
    }

}
