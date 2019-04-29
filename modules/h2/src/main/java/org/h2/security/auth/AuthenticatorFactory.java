/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

/**
 * Authenticator factory
 */
public class AuthenticatorFactory {

    /**
     * Factory method.
     * @return authenticator instance.
     */
    public static Authenticator createAuthenticator() {
        return DefaultAuthenticator.getInstance();
    }
}
