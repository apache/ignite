package org.apache.ignite.internal.processors.security;

import static sun.security.util.SecurityConstants.MODIFY_THREADGROUP_PERMISSION;
import static sun.security.util.SecurityConstants.MODIFY_THREAD_PERMISSION;

/**
 * Default Ignite Security Manager.
 */
public class IgniteSecurityManager extends SecurityManager {
    /** {@inheritDoc} */
    @Override public void checkAccess(Thread t) {
        super.checkAccess(t);

        checkPermission(MODIFY_THREAD_PERMISSION);
    }

    /** {@inheritDoc} */
    @Override public void checkAccess(ThreadGroup g) {
        super.checkAccess(g);

        checkPermission(MODIFY_THREADGROUP_PERMISSION);
    }
}
