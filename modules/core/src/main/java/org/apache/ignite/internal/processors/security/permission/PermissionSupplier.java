package org.apache.ignite.internal.processors.security.permission;

import java.security.Permission;

public interface PermissionSupplier {

    public Permission permission();
}
