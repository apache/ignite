package org.apache.ignite.internal.processors.service.permission;

import java.security.Permission;
import org.apache.ignite.internal.processors.security.permission.ActionPermissionCollection;

public class ServicePermissionCollection extends ActionPermissionCollection<ServicePermission> {
    private static final long serialVersionUID = 4690740871290781542L;

    @Override protected ServicePermission newPermission(String name, String actions) {
        return new ServicePermission(name, actions);
    }

    @Override protected boolean checkPermissionType(Permission permission) {
        return permission instanceof ServicePermission;
    }
}
