package org.apache.ignite.p2p.security;

import static sun.security.util.SecurityConstants.MODIFY_THREADGROUP_PERMISSION;
import static sun.security.util.SecurityConstants.MODIFY_THREAD_PERMISSION;

/**
 * Чтобы задать более строгое ограничение на создание новых потоков,
 * необходимо переоперделить два метода.
 */
public class TestIgniteSecurityManager extends SecurityManager {
    @Override public void checkAccess(Thread t) {
        super.checkAccess(t);

        checkPermission(MODIFY_THREAD_PERMISSION);
    }

    @Override public void checkAccess(ThreadGroup g) {
        super.checkAccess(g);

        checkPermission(MODIFY_THREADGROUP_PERMISSION);
    }
}
