package org.apache.ignite.p2p.security;

import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.PrivilegedActionException;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import org.junit.BeforeClass;
import org.junit.Test;
import sun.security.util.SecurityConstants;

public class ThreadStartTest {

    @BeforeClass
    public static void setup() {
        if (System.getSecurityManager() == null) {
            Policy.setPolicy(new Policy() {
                @Override
                public PermissionCollection getPermissions(CodeSource cs) {
                    Permissions result = new Permissions();

                    result.add(new AllPermission());

                    return result;
                }
            });

            System.setSecurityManager(new TestIgniteSecurityManager());
        }
    }

    //Создает и запускает новый поток.
    private final Runnable runnable = () -> new Thread(
        () -> System.out.println("Hi from new thread!")
    ).start();

    @Test(expected = AccessControlException.class)
    public void testShouldThrowException() throws Exception {
        //запуск runnable без прав старта потока.
        runWithPermissions(runnable);
    }

    @Test
    public void testShouldStartNewThread() throws Exception {
        //запуск runnable с правами, позволяющими стартовать новый поток.
        runWithPermissions(runnable,
            SecurityConstants.MODIFY_THREADGROUP_PERMISSION,
            SecurityConstants.MODIFY_THREAD_PERMISSION
        );
    }

    private void runWithPermissions(Runnable r, Permission... permissions) throws PrivilegedActionException {
        Permissions perms = new Permissions();

        for (Permission p : permissions)
            perms.add(p);

        ProtectionDomain domain = new ProtectionDomain(new CodeSource(null, (Certificate[])null), perms);

        AccessControlContext ctx = new AccessControlContext(new ProtectionDomain[] {domain});

        AccessController.doPrivileged(new RunnableAction(r), ctx);
    }

}