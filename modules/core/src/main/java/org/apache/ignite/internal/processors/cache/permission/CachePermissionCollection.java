package org.apache.ignite.internal.processors.cache.permission;

import java.security.Permission;
import org.apache.ignite.internal.processors.security.permission.ActionPermissionCollection;

/**
 *
 */
final class CachePermissionCollection extends ActionPermissionCollection<CachePermission> {
    private static final long serialVersionUID = 4462559620716988841L;

    @Override protected CachePermission newPermission(String name, String actions) {
        return new CachePermission(name, actions);
    }

    @Override protected boolean checkPermissionType(Permission permission) {
        return permission instanceof CachePermission;
    }
}