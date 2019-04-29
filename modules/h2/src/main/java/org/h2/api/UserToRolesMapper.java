/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.api;

import java.util.Collection;

import org.h2.security.auth.AuthenticationException;
import org.h2.security.auth.AuthenticationInfo;
import org.h2.security.auth.Configurable;

/**
 * A class that implement this interface can be used during authentication to
 * map external users to database roles.
 * <p>
 * <b>This feature is experimental and subject to change</b>
 * </p>
 */
public interface UserToRolesMapper extends Configurable {

    /**
     * Map user identified by authentication info to a set of granted roles.
     *
     * @param authenticationInfo
     *            authentication information
     * @return list of roles to be assigned to the user temporary
     * @throws AuthenticationException
     *             on authentication exception
     */
    Collection<String> mapUserToRoles(AuthenticationInfo authenticationInfo) throws AuthenticationException;
}
