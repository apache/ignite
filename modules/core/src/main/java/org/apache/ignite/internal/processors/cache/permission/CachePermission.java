package org.apache.ignite.internal.processors.cache.permission;

import java.security.PermissionCollection;
import org.apache.ignite.internal.processors.security.permission.ActionDefs;
import org.apache.ignite.internal.processors.security.permission.ActionPermission;

public final class CachePermission extends ActionPermission {
    private static final long serialVersionUID = -2039127669186742977L;


    public static final String CREATE = "create";
    public static final String DESTROY = "destroy";
    public static final String GET = "get";
    public static final String PUT = "put";
    public static final String REMOVE = "remove";
    public static final String ALL = "*";

    private static final int CODE_CREATE = 0x01;
    private static final int CODE_DESTROY = 0x02;
    private static final int CODE_GET = 0x04;
    private static final int CODE_PUT = 0x08;
    private static final int CODE_REMOVE = 0x10;
    private static final int CODE_ALL = CODE_CREATE | CODE_DESTROY | CODE_GET | CODE_PUT | CODE_REMOVE;

    private static final ActionDefs ACTION_DEFS = ActionDefs.builder()
        .add(CODE_CREATE, CREATE)
        .add(CODE_DESTROY, DESTROY)
        .add(CODE_GET, GET)
        .add(CODE_PUT, PUT)
        .add(CODE_REMOVE, REMOVE)
        .add(CODE_ALL, ALL)
        .build();

    /**
     * Constructs a permission with the specified name.
     *
     * @param name name of the Permission object being created.
     */
    public CachePermission(String name, String actions) {
        super(name, actions);
    }

    @Override public PermissionCollection newPermissionCollection() {
        return new CachePermissionCollection();
    }

    @Override protected ActionDefs actionDefs() {
        return ACTION_DEFS;
    }

    @Override protected int codeAll() {
        return CODE_ALL;
    }
}