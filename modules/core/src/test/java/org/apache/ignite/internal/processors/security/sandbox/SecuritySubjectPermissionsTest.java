/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.security.sandbox;

import java.io.FilePermission;
import java.io.SerializablePermission;
import java.lang.management.ManagementPermission;
import java.lang.reflect.ReflectPermission;
import java.net.SocketPermission;
import java.security.AccessControlException;
import java.security.BasicPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.SecurityPermission;
import java.util.PropertyPermission;
import javax.management.MBeanPermission;
import javax.management.MBeanServerPermission;
import javax.management.MBeanTrustPermission;
import javax.net.ssl.SSLPermission;
import org.apache.ignite.Ignite;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * The security subject cannot have access higher than Ignite himself.
 */
public class SecuritySubjectPermissionsTest extends AbstractSandboxTest {
    /** */
    @Test
    public void test() throws Exception {
        Ignite srv = startGrid("srv", ALLOW_ALL, false);

        Permissions perms = new Permissions();
        // Permission that Ignite and subject do have.
        perms.add(new TestPermission("common"));
        // Permission that Ignite does not have.
        perms.add(new TestPermission("only_subject"));

        Ignite clnt = startGrid("clnt", ALLOW_ALL, perms, true);

        srv.cluster().active(true);

        clnt.compute().broadcast(() -> securityManager().checkPermission(new TestPermission("common")));

        assertThrowsWithCause(() -> clnt.compute().broadcast(
            () -> {
                securityManager().checkPermission(new TestPermission("only_subject"));

                fail();
            }),
            AccessControlException.class);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        if (System.getSecurityManager() == null) {
            Policy.setPolicy(new Policy() {
                @Override public PermissionCollection getPermissions(CodeSource cs) {
                    Permissions res = new Permissions();

                    res.add(new RuntimePermission("*"));
                    res.add(new MBeanServerPermission("*"));
                    res.add(new MBeanPermission("*", "*"));
                    res.add(new MBeanTrustPermission("*"));
                    res.add(new ReflectPermission("*"));
                    res.add(new SSLPermission("*"));
                    res.add(new ManagementPermission("monitor"));
                    res.add(new ManagementPermission("control"));
                    res.add(new SerializablePermission("*"));
                    res.add(new SecurityPermission("*"));
                    res.add(new SocketPermission("*", "connect,accept,listen,resolve"));
                    res.add(new FilePermission("<<ALL FILES>>", "read,write,delete,execute,readlink"));
                    res.add(new PropertyPermission("*", "read,write"));

                    res.add(new TestPermission("common"));

                    return res;
                }
            });

            System.setSecurityManager(new SecurityManager());

            setupSM = true;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        if (setupSM) {
            System.setSecurityManager(null);
            Policy.setPolicy(null);
        }
    }

    /** */
    private SecurityManager securityManager() {
        SecurityManager sm = System.getSecurityManager();

        assertNotNull(sm);

        return sm;
    }

    /** Permission for test. */
    public static class TestPermission extends BasicPermission {
        /** */
        public TestPermission(String name) {
            super(name);
        }
    }
}
