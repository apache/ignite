package org.apache.ignite.internal.processors.security.impl;

import java.security.AllPermission;
import java.security.Permission;
import java.security.Permissions;
import org.apache.ignite.internal.processors.security.SecurityConstants;

public class PermissionsBuilder {

    public static PermissionsBuilder create() {
        return new PermissionsBuilder();
    }

    public static PermissionsBuilder create(boolean joinAsServer) {
        PermissionsBuilder res = new PermissionsBuilder();

        if (joinAsServer)
            res.add(SecurityConstants.JOIN_AS_SERVER_PERMISSION);

        return res;

    }

    public static Permissions createAllowAll() {
        Permissions res = new Permissions();

        res.add(new AllPermission());

        return res;
    }

    private final Permissions permissions;

    private PermissionsBuilder() {
        permissions = new Permissions();
    }

    public PermissionsBuilder add(Permission perm) {
        permissions.add(perm);

        return this;
    }

    public Permissions get() {
        return permissions;
    }

}
