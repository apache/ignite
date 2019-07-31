package org.apache.ignite.internal.processors.security;

import java.security.Permission;
import org.apache.ignite.internal.processors.security.permissions.AccessExtendedApiPermission;

/**
 * Ignite security constants.
 */
public final class IgniteSecurityConstants {
    /** Permission to execute set of Ignitionex.grid methods. */
    public static final Permission IGNITIONEX_GRID_PERMISSION =
        new AccessExtendedApiPermission("ignitionex.grid");

    /** Permission to add listener by Ignitionex.addListener method. */
    public static final Permission IGNITIONEX_ADDLISTENER_PERMISSION =
        new AccessExtendedApiPermission("ignitionex.addListener");
    /** Permission to remove listener by Ignitionex.removeListener method. */
    public static final Permission IGNITIONEX_REMOVELISTENER_PERMISSION =
        new AccessExtendedApiPermission("ignitionex.removeListener");

    /** Ctor. */
    private IgniteSecurityConstants() {
        // No-op.
    }
}
