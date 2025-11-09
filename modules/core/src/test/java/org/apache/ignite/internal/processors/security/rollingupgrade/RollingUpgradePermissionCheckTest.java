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

package org.apache.ignite.internal.processors.security.rollingupgrade;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Test rolling upgrade permissions. */
public class RollingUpgradePermissionCheckTest extends AbstractSecurityTest {
    /**
     * @throws Exception If failed.
     */
    @Test public void testRollingUpgradePermissions() throws Exception {
        IgniteEx node = startGrid("server_test_node", SecurityPermissionSetBuilder.create().defaultAllowAll(false).build(), false);

        for (IgniteThrowableConsumer<IgniteEx> c : operations()) {
            Throwable throwable = assertThrows(log, () -> c.accept(node), IgniteException.class, "Authorization failed");

            assertTrue(X.hasCause(throwable, SecurityException.class));
        }
    }

    /**
     * @return Collection of operations to manage rolling upgrade.
     */
    private List<IgniteThrowableConsumer<IgniteEx>> operations() {
        return Arrays.asList(
            ign -> ign.context().rollingUpgrade().enable(IgniteProductVersion.fromString("2.18.0")),
            ign -> ign.context().rollingUpgrade().disable()
        );
    }
}
