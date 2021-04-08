package org.apache.ignite.compute;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import org.apache.ignite.internal.processors.security.IgniteComponentPermission;

import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.CANCEL;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.EXECUTE;

public class ComputePermission extends IgniteComponentPermission {

    private static final long serialVersionUID = 0L;

    private static final int MSK_EXECUTE = 0x1;
    private static final int MSK_CANCEL = 0x2;

    public ComputePermission(String name, String actions) {
        super(name, actions);

        mask = getMask(actions);
    }

    ComputePermission(String name, int mask) {
        super(name, null);

        this.mask = mask;
    }

    @Override public boolean implies(Permission p) {
        if (!(p instanceof ComputePermission))
            return false;

        return super.implies(p);
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof ComputePermission))
            return false;

        return super.equals(obj);
    }

    @Override public PermissionCollection newPermissionCollection() {
        return new IgniteComputePermissionCollection();
    }

    @Override protected String actions(int m) {
        StringJoiner sj = new StringJoiner(",");

        if ((m & MSK_EXECUTE) == MSK_EXECUTE)
            sj.add(EXECUTE);

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

            if (EXECUTE.equals(tok))
                res |= MSK_EXECUTE;
            else if (CANCEL.equals(tok))
                res |= MSK_CANCEL;
            else
                throw new IllegalStateException("Unknown action: " + tok);
        }

        return res;
    }

    private static final class IgniteComputePermissionCollection extends IgniteComponentPermissionCollection<ComputePermission> {

        private static final long serialVersionUID = 0L;

        @Override protected boolean isInstanceOf(Permission p) {
            return p instanceof ComputePermission;
        }

        @Override protected ComputePermission newInstance(String name, int mask) {
            return new ComputePermission(name, mask);
        }
    }
}
