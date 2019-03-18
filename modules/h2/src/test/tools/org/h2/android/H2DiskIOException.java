/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

/**
 * This exception is thrown when there was a IO exception.
 */
public class H2DiskIOException extends H2Exception {
    private static final long serialVersionUID = 1L;

    H2DiskIOException() {
        super();
    }

    H2DiskIOException(String error) {
        super(error);
    }
}
