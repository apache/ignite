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
import org.apache.ignite.internal.IgniteDiagnosticInfo;
import org.junit.BeforeClass;
import org.junit.Test;

public class AccessToPackageTest {

    @BeforeClass
    public static void setup(){
        java.security.Security.setProperty("package.access", "org.apache.ignite.internal");

        if (System.getSecurityManager() == null) {
            Policy.setPolicy(new Policy() {
                @Override
                public PermissionCollection getPermissions(CodeSource cs) {
                    Permissions result = new Permissions();

                    result.add(new AllPermission());

                    return result;
                }
            });

            System.setSecurityManager(new SecurityManager());
        }
    }

    //Создает класс из внутреннего пакета ignite.
    private final Runnable runnable = () -> new IgniteDiagnosticInfo(null);

    @Test(expected = AccessControlException.class)
    public void testShouldThrowExpetion() throws Exception {
        //запуск runnable без права доступа к внутреннему пакету ignite.
        runWithPermissions(runnable);
    }

    @Test
    public void testShouldRunWhenHasPermission() throws Exception {
        //запуск runnable с правом доступа к внутреннему пакету ignite.
        runWithPermissions(runnable,
            new RuntimePermission("accessClassInPackage.org.apache.ignite.internal"));
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
