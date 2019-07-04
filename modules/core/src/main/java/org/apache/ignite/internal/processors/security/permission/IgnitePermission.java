package org.apache.ignite.internal.processors.security.permission;

import java.security.BasicPermission;

public final class IgnitePermission extends BasicPermission {
    private static final long serialVersionUID = 5638532084204314536L;

    public IgnitePermission(String name) {
        super(name);
    }

    public IgnitePermission(String name, String actions) {
        super(name, actions);
    }
}
