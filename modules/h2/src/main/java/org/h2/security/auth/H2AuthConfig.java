/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import java.util.ArrayList;
import java.util.List;

/**
 * Describe configuration of H2 DefaultAuthenticator.
 */
public class H2AuthConfig {

    private boolean allowUserRegistration=true;
    private boolean createMissingRoles=true;
    private List<RealmConfig> realms;
    private List<UserToRolesMapperConfig> userToRolesMappers;

    /**
     * Allow user registration flag. If set to {@code true}
     * creates external users in the database if not present.
     *
     * @return {@code true} in case user registration is allowed,
     *          otherwise returns {@code false}.
     */
    public boolean isAllowUserRegistration() {
        return allowUserRegistration;
    }

    /**
     * @param allowUserRegistration Allow user registration flag.
     */
    public void setAllowUserRegistration(boolean allowUserRegistration) {
        this.allowUserRegistration = allowUserRegistration;
    }

    /**
     * When set create roles not found in the database. If not set roles not
     * found in the database are silently skipped.
     * @return {@code true} if the flag is set, otherwise returns {@code false}.
     */
    public boolean isCreateMissingRoles() {
        return createMissingRoles;
    }

    /**
     * When set create roles not found in the database. If not set roles not
     * found in the database are silently skipped
     * @param createMissingRoles missing roles flag.
     */
    public void setCreateMissingRoles(boolean createMissingRoles) {
        this.createMissingRoles = createMissingRoles;
    }

    /**
     * Gets configuration of authentication realms.
     *
     * @return configuration of authentication realms.
     */
    public List<RealmConfig> getRealms() {
        if (realms == null) {
            realms = new ArrayList<>();
        }
        return realms;
    }

    /**
     * Sets configuration of authentication realms.
     *
     * @param realms configuration of authentication realms.
     */
    public void setRealms(List<RealmConfig> realms) {
        this.realms = realms;
    }

    /**
     * Gets configuration of the mappers external users to database roles.
     *
     * @return configuration of the mappers external users to database roles.
     */
    public List<UserToRolesMapperConfig> getUserToRolesMappers() {
        if (userToRolesMappers == null) {
            userToRolesMappers = new ArrayList<>();
        }
        return userToRolesMappers;
    }

    /**
     * Sets configuration of the mappers external users to database roles.
     *
     * @param userToRolesMappers configuration of the mappers external users to database roles.
     */
    public void setUserToRolesMappers(List<UserToRolesMapperConfig> userToRolesMappers) {
        this.userToRolesMappers = userToRolesMappers;
    }
}
