package org.apache.ignite.internal.processors.task.permission;

import java.security.PermissionCollection;
import org.apache.ignite.internal.processors.security.permission.ActionDefs;
import org.apache.ignite.internal.processors.security.permission.ActionPermission;

public class TaskPermission extends ActionPermission {
    private static final long serialVersionUID = -1495607228802790333L;

    /** Execute action. */
    public static final String EXECUTE = "execute";

    /** Cancel action. */
    public static final String CANCEL = "cancel";

    /** All actions (execute, cancel). */
    public static final String ALL = "*";

    /** Execute action. */
    private static final int CODE_EXECUTE = 0x1;

    /** Cancel action. */
    private static final int CODE_CANCEL = 0x2;

    /** All actions (execute, cancel). */
    private static final int CODE_ALL = CODE_EXECUTE | CODE_CANCEL;

    private static final ActionDefs ACTION_DEFS = ActionDefs.builder()
        .add(CODE_EXECUTE, EXECUTE)
        .add(CODE_CANCEL, CANCEL)
        .add(CODE_ALL, ALL)
        .build();

    public TaskPermission(String name, String actions) {
        super(name, actions);
    }

    /**
     * Returns a new PermissionCollection object for storing TaskPermission objects.
     * <p>
     *
     * @return a new PermissionCollection object suitable for storing TaskPermissions.
     */
    @Override public PermissionCollection newPermissionCollection() {
        return new TaskPermissionCollection();
    }

    @Override protected int codeAll() {
        return CODE_ALL;
    }

    @Override protected ActionDefs actionDefs() {
        return ACTION_DEFS;
    }
}
