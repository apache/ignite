/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import org.h2.engine.ConnectionInfo;
import org.h2.util.StringUtils;

/**
 * Input data for authenticators; it wraps ConnectionInfo
 */
public class AuthenticationInfo {

    private ConnectionInfo connectionInfo;

    private String password;

    private String realm;

    /**
     * Can be used by authenticator to hold information.
     */
    Object nestedIdentity;

    public AuthenticationInfo(ConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
        this.realm = connectionInfo.getProperty("AUTHREALM", null);
        if (this.realm != null) {
            this.realm = StringUtils.toUpperEnglish(this.realm);
        }
        this.password = connectionInfo.getProperty("AUTHZPWD", null);
    }

    public String getUserName() {
        return connectionInfo.getUserName();
    }

    public String getRealm() {
        return realm;
    }

    public String getPassword() {
        return password;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public String getFullyQualifiedName() {
        if (realm == null) {
            return connectionInfo.getUserName();
        } else {
            return connectionInfo.getUserName() + "@" + realm;
        }
    }

    /**
     * Gets nested identity object that can be used by authenticator to hold information.
     *
     * @return nested identity object.
     */
    public Object getNestedIdentity() {
        return nestedIdentity;
    }

    /**
     * Method used by authenticators to hold information about authenticated
     * user
     *
     * @param nestedIdentity
     *            = nested identity object
     */
    public void setNestedIdentity(Object nestedIdentity) {
        this.nestedIdentity = nestedIdentity;
    }

    /**
     * Clean authentication data.
     */
    public void clean() {
        this.password = null;
        this.nestedIdentity = null;
        connectionInfo.cleanAuthenticationInfo();
    }

}
