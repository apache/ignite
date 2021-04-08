package org.apache.ignite.internal.processors.security;

import java.security.AllPermission;
import java.security.Permission;
import java.security.Permissions;
import org.apache.ignite.cluster.IgniteClusterPermission;

public final class IgniteSecurityConstants {
    private IgniteSecurityConstants() {
        // No-op.
    }

    public static final Permissions ALLOW_ALL_PERMISSIONS = new Permissions();
    static {
        ALLOW_ALL_PERMISSIONS.add(new AllPermission());
        ALLOW_ALL_PERMISSIONS.setReadOnly();
    }

    /** Actions. */
    public static final String CREATE = "create";
    public static final String DESTROY = "destroy";
    public static final String GET = "get";
    public static final String PUT = "put";
    public static final String REMOVE = "remove";
    public static final String DEPLOY = "deploy";
    public static final String INVOKE = "invoke";
    public static final String EXECUTE = "execute";
    public static final String CANCEL = "cancel";


    /** Permissions. */
    public static final Permission JOIN_AS_SERVER = new IgniteClusterPermission("joinAsServer");
    public static final Permission ADMIN_SNAPSHOT = new IgniteClusterPermission("adminSnapshot");
    public static final Permission ADMIN_METADATA_OPS = new IgniteClusterPermission("adminMetadataOps");
    public static final Permission ADMIN_OPS = new IgniteClusterPermission("adminOps");
    public static final Permission ADMIN_READ_DISTRIBUTED_PROPERTY = new IgniteClusterPermission("adminReadDistributedProperty");
    public static final Permission ADMIN_WRITE_DISTRIBUTED_PROPERTY = new IgniteClusterPermission("adminWriteDistributedProperty");

}
