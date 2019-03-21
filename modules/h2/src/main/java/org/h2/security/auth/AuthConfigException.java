/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

/**
 * Exception thrown when an issue occurs during the authentication configuration
 *
 */
public class AuthConfigException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public AuthConfigException() {
        super();
    }

    public AuthConfigException(String message) {
        super(message);
    }

    public AuthConfigException(Throwable cause) {
        super(cause);
    }

    public AuthConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
