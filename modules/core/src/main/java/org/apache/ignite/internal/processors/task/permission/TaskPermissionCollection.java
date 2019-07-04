package org.apache.ignite.internal.processors.task.permission;

import java.security.Permission;
import org.apache.ignite.internal.processors.security.permission.ActionPermissionCollection;

final class TaskPermissionCollection extends ActionPermissionCollection<TaskPermission> {
    private static final long serialVersionUID = -7869126823032374453L;

    public TaskPermissionCollection() {
    }

    @Override protected TaskPermission newPermission(String name, String actions) {
        return new TaskPermission(name, actions);
    }

    @Override protected boolean checkPermissionType(Permission permission) {
        return permission instanceof TaskPermission;
    }
}
