/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import android.database.SQLException;

/**
 * This exception is thrown when there is a syntax error or similar problem.
 */
public class H2Exception extends SQLException {
    private static final long serialVersionUID = 1L;

    public H2Exception() {
        super();
    }

    public H2Exception(String error) {
        super(error);
    }
}
