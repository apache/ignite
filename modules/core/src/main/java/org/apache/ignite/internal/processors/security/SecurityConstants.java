package org.apache.ignite.internal.processors.security;

import org.apache.ignite.internal.processors.security.permission.IgnitePermission;

public final class SecurityConstants {

    public static final IgnitePermission JOIN_AS_SERVER_PERMISSION = new IgnitePermission("joinAsSever");

    public static final IgnitePermission EVENTS_ENABLE_PERMISSION = new IgnitePermission("eventsEnable");
    public static final IgnitePermission EVENTS_DISABLE_PERMISSION = new IgnitePermission("eventsDisable");

    public static final IgnitePermission VISOR_ADMIN_VIEW_PERMISSION = new IgnitePermission("visor.adminView");
    public static final IgnitePermission VISOR_ADMIN_QUERY_PERMISSION = new IgnitePermission("visor.adminQuery");
    public static final IgnitePermission VISOR_ADMIN_CACHE_PERMISSION = new IgnitePermission("visor.adminCache");
    public static final IgnitePermission VISOR_ADMIN_OPS_PERMISSION = new IgnitePermission("visor.adminOps");

    private SecurityConstants() {
        // No-op
    }
}
