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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.IgniteSecurityConstants;
import org.apache.ignite.internal.processors.security.impl.PermissionsBuilder;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 *
 */
public class ConstraintIgnitionTest extends AbstractSandboxTest {
    /**
     *
     */
    private static final IgniteRunnable INJECTION_TEST_RUNNABLE = new IgniteRunnable() {
        /** */
        @IgniteInstanceResource
        private Ignite localIgnite;

        @Override public void run() {
            assertNotNull(localIgnite);

            assertFalse(localIgnite instanceof IgniteEx);
        }
    };

    /**
     *
     */
    @Before
    public void setUp() throws Exception {
        Ignite srv = startGrid(SRV, ALLOW_ALL, false);

        startGrid(CLNT_ALLOWED, ALLOW_ALL,
            PermissionsBuilder.create()
                .add(IgniteSecurityConstants.IGNITIONEX_GRID_PERMISSION).get(), true);

        startGrid(CLNT_FORBIDDEN, ALLOW_ALL, true);

        srv.cluster().active(true);
    }

    /**
     *
     */
    @After
    public void tearDown() {
        G.stopAll(true);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        Ignite clntForbidden = grid(CLNT_FORBIDDEN);

        IgniteCompute compute = clntForbidden.compute(clntForbidden.cluster().forRemotes());

        // Injected instance of ignite is not instance of IgniteEx.
        compute.broadcast(INJECTION_TEST_RUNNABLE);

        //Ignite accessed by static method is not instance of IgniteEx.
        compute.broadcast(() -> {
            Ignite ignite = Ignition.localIgnite();

            assertNotNull(ignite);

            assertFalse(ignite instanceof IgniteEx);
        });

        //Lambda that do not have permission, cannot call to IgnitionEx.localIgnite method directly.
        assertThrowsWithCause(() -> compute.broadcast(IgnitionEx::localIgnite), SecurityException.class);


        Ignite clntAllowed = grid(CLNT_ALLOWED);
        //Lambda is executed behalf of a client that has the IGNITIONEX_GRID_PERMISSION permission.
        clntAllowed.compute(clntAllowed.cluster().forRemotes())
            .broadcast(() -> assertNotNull(IgnitionEx.localIgnite()));
    }

}
