package org.apache.ignite.cache;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import org.apache.ignite.internal.processors.security.IgniteComponentPermission;

import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.CREATE;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.DESTROY;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.GET;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.PUT;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.REMOVE;

public class CachePermission extends IgniteComponentPermission {

    private static String cacheName(String input) {
        // Currently, null means for all caches.
        return input == null ? "*" : input;
    }

    private static final long serialVersionUID = 0L;

    private static final int MSK_CREATE = 0x1;
    private static final int MSK_DESTROY = 0x2;
    private static final int MSK_GET = 0x4;
    private static final int MSK_PUT = 0x8;
    private static final int MSK_REMOVE = 0x10;

    public CachePermission(String name, String action) {
        super(cacheName(name), action);

        mask = getMask(action);
    }

    CachePermission(String name, int mask) {
        super(name, null);

        this.mask = mask;
    }

    @Override public boolean implies(Permission permission) {
        if (!(permission instanceof CachePermission))
            return false;

        return super.implies(permission);
    }

    @Override public PermissionCollection newPermissionCollection() {
        return new IgniteCachePermissionCollections();
    }

    @Override protected String actions(int m) {
        StringJoiner sj = new StringJoiner(",");

        if ((m & MSK_CREATE) == MSK_CREATE)
            sj.add(CREATE);

        if ((m & MSK_DESTROY) == MSK_DESTROY)
            sj.add(DESTROY);

        if ((m & MSK_GET) == MSK_GET)
            sj.add(GET);

        if ((m & MSK_PUT) == MSK_PUT)
            sj.add(PUT);

        if ((m & MSK_REMOVE) == MSK_REMOVE)
            sj.add(REMOVE);

        return sj.toString();
    }

    @Override protected int getMask(String actions) {
        int res = MSK_NONE;

        if (actions == null)
            return res;

        StringTokenizer st = new StringTokenizer(actions, ",\t ");
        while (st.hasMoreTokens()) {
            String tok = st.nextToken();
            switch (tok) {
                case CREATE:
                    res |= MSK_CREATE;
                    break;

                case DESTROY:
                    res |= MSK_DESTROY;
                    break;

                case GET:
                    res |= MSK_GET;
                    break;

                case PUT:
                    res |= MSK_PUT;
                    break;

                case REMOVE:
                    res |= MSK_REMOVE;
                    break;

                default:
                    throw new IllegalStateException("Unknown action: " + tok);
            }
        }
        return res;
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof CachePermission))
            return false;

        return super.equals(obj);
    }

    private static final class IgniteCachePermissionCollections extends IgniteComponentPermissionCollection<CachePermission> {

        private static final long serialVersionUID = 0L;

        @Override protected boolean isInstanceOf(Permission p) {
            return p instanceof CachePermission;
        }

        @Override protected CachePermission newInstance(String name, int mask) {
            return new CachePermission(name, mask);
        }
    }
}
