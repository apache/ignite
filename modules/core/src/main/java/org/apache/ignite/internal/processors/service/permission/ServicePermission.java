package org.apache.ignite.internal.processors.service.permission;

import java.security.PermissionCollection;
import org.apache.ignite.internal.processors.security.permission.ActionDefs;
import org.apache.ignite.internal.processors.security.permission.ActionPermission;

public final class ServicePermission extends ActionPermission {
    private static final long serialVersionUID = -261094925868648825L;

    public static final String DEPLOY = "deploy";
    public static final String INVOKE = "invoke";
    public static final String CANCEL = "cancel";
    public static final String ALL = "*";

    private static final int CODE_DEPLOY = 0x1;
    private static final int CODE_INVOKE = 0x2;
    private static final int CODE_CANCEL = 0x4;
    private static final int CODE_ALL = CODE_DEPLOY | CODE_INVOKE | CODE_CANCEL;

    private static final ActionDefs ACTION_DEFS = ActionDefs.builder()
        .add(CODE_DEPLOY, DEPLOY)
        .add(CODE_INVOKE, INVOKE)
        .add(CODE_CANCEL, CANCEL)
        .add(CODE_ALL, ALL)
        .build();

    public ServicePermission(String name, String actions) {
        super(name, actions);
    }

    @Override public PermissionCollection newPermissionCollection() {
        return new ServicePermissionCollection();
    }

    @Override protected ActionDefs actionDefs() {
        return ACTION_DEFS;
    }

    @Override protected int codeAll() {
        return CODE_ALL;
    }
}
