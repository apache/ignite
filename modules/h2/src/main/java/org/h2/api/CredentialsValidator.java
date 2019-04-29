/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.api;

import org.h2.security.auth.AuthenticationInfo;
import org.h2.security.auth.Configurable;

/**
 * A class that implement this interface can be used to validate credentials
 * provided by client.
 * <p>
 * <b>This feature is experimental and subject to change</b>
 * </p>
 */
public interface CredentialsValidator extends Configurable {

    /**
     * Validate user credential.
     *
     * @param authenticationInfo
     *            = authentication info
     * @return true if credentials are valid, otherwise false
     * @throws Exception
     *             any exception occurred (invalid credentials or internal
     *             issue) prevent user login
     */
    boolean validateCredentials(AuthenticationInfo authenticationInfo) throws Exception;

}
