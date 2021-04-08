package org.apache.ignite.services;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import org.apache.ignite.internal.processors.security.IgniteComponentPermission;

import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.CANCEL;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.DEPLOY;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.INVOKE;

public class ServicePermission extends IgniteComponentPermission {

    private static final long serialVersionUID = 0L;

    private static final int MSK_DEPLOY = 0x1;
    private static final int MSK_INVOKE = 0x2;
    private static final int MSK_CANCEL = 0x4;

    public ServicePermission(String name, String actions) {
        super(name, actions);

        mask = getMask(actions);
    }

    ServicePermission(String name, int mask) {
        super(name, null);

        this.mask = mask;
    }

    @Override public boolean implies(Permission p) {
        if (!(p instanceof ServicePermission))
            return false;

        return super.implies(p);
    }


    @Override public boolean equals(Object obj) {
        if (!(obj instanceof ServicePermission))
            return false;

        return super.equals(obj);
    }

    @Override public PermissionCollection newPermissionCollection() {
        return new IgniteServicePermissionCollection();
    }

    @Override protected String actions(int m) {
        StringJoiner sj = new StringJoiner(",");

        if ((m & MSK_DEPLOY) == MSK_DEPLOY)
            sj.add(DEPLOY);

        if ((m & MSK_INVOKE) == MSK_INVOKE)
            sj.add(INVOKE);

        if ((m & MSK_CANCEL) == MSK_CANCEL)
            sj.add(CANCEL);

        return sj.toString();
    }

    @Override protected int getMask(String actions) {
        int res = MSK_NONE;

        if (actions == null)
            return res;

        StringTokenizer st = new StringTokenizer(actions, ",\t ");
        while (st.hasMoreTokens()) {
            String tok = st.nextToken();

            if (DEPLOY.equals(tok))
                res |= MSK_DEPLOY;
            else if (INVOKE.equals(tok))
                res |= MSK_INVOKE;
            else if (CANCEL.equals(tok))
                res |= MSK_CANCEL;
            else
                throw new IllegalStateException("Unknown action: " + tok);
        }

        return res;
    }

    private static final class IgniteServicePermissionCollection extends IgniteComponentPermissionCollection<ServicePermission> {

        private static final long serialVersionUID = 0L;

        @Override protected boolean isInstanceOf(Permission p) {
            return p instanceof ServicePermission;
        }

        @Override protected ServicePermission newInstance(String name, int mask) {
            return new ServicePermission(name, mask);
        }
    }
}
