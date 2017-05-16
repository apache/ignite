package org.apache.ignite.util.mbeans;

import org.apache.ignite.cache.IgniteWarmupClosureSelfTest;
import org.apache.ignite.internal.util.IgniteUtils;

import java.io.File;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_MBEANS;

/**
 * runs a test with SecurityManager installed
 */
public class IgniteDisableMBeansTestMain {
    static String policyFileName = "src/test/resources/org/apache/ignite/util/mbeans/mbeans-disallowed.policy";

    public static void main(String... args) throws Exception {
        SecurityManager sm = System.getSecurityManager();

        System.out.println("SecurityManager exists: " +  (sm != null));

        if (sm == null) {
            // that is, method main() was called directly, not as a process from IgniteDisableMBeansTest
            // prepare all the properties manually
            String fn = System.getProperty("java.security.policy");

            if (fn == null) {
                File policy = new File(policyFileName);

                boolean exists = policy.exists();

                if (!exists) {
                    policyFileName = "modules/core/" + policyFileName;

                    policy = new File(policyFileName);

                    exists = policy.exists();

                    if (exists)
                        System.setProperty("java.security.policy", policyFileName);

                }

                if (!exists) {
                    String pwd = System.getProperty("user.dir");

                    System.out.println("could not find "+policyFileName+" in "+ pwd);

                    System.exit(117);
                }
            }

            sm = new SecurityManager();

            System.setSecurityManager(sm);

            System.out.println("SecurityManager created");

            System.setProperty(IGNITE_DISABLE_MBEANS, "true");

            IgniteUtils.IGNITE_DISABLE_MBEANS = true;
        }

        try {
            System.out.println("Test started");

            IgniteWarmupClosureSelfTest test = new IgniteWarmupClosureSelfTest();

            test.testWarmupClosure();

            test.afterTestsStopped();

            System.out.println("Test passed");
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    static class IgniteWarmupClosureSelfTest extends org.apache.ignite.cache.IgniteWarmupClosureSelfTest {
        @Override
        protected void afterTestsStopped() throws Exception {
            super.afterTestsStopped();
        }
    }
}
