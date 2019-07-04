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

package org.apache.ignite.internal.processors.security.compute.sandbox;

import java.security.AccessControlException;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.util.PropertyPermission;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.IgniteSecurityManager;
import org.apache.ignite.internal.processors.security.impl.PermissionsBuilder;
import org.apache.ignite.internal.processors.task.permission.TaskPermission;
import org.apache.ignite.lang.IgniteCallable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** . */
    public class DistributedClosureSandboxTest extends AbstractSecurityTest {

    private static boolean setupSM = false;

    private static final IgniteCallable<String> GET_PROPERTY_CALLABLE = () -> System.getProperty("java.home");

    /** . */
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

            System.setSecurityManager(new IgniteSecurityManager());

            setupSM = true;
        }
    }

    @AfterClass
    public void tearDown() throws Exception {
        if (setupSM) {
            System.setSecurityManager(null);
            Policy.setPolicy(null);
        }
    }

    /** . */
    @Test
    public void test() throws Exception {
        PermissionsBuilder pb = PermissionsBuilder.create(true)
            .add(new TaskPermission(getClass().getPackage().getName() + ".*", "execute"));

        Ignite srvForbidden = startGrid("srv_forbidden", pb.get(), false);

        Ignite srvAllowed = startGrid("srv_allowed",
            pb.add(new PropertyPermission("java.home", "read")).get(), false);

        srvAllowed.cluster().active(true);

        String javaHome = srvAllowed.compute(srvAllowed.cluster().forLocal()).call(GET_PROPERTY_CALLABLE);

        assertThat(javaHome, is(System.getProperty("java.home")));

        assertThrowsWithCause(
            () -> srvForbidden.compute(srvForbidden.cluster().forLocal()).call(GET_PROPERTY_CALLABLE),
            AccessControlException.class
        );
    }
}
