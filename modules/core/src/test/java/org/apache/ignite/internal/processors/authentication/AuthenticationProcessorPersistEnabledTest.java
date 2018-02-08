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

package org.apache.ignite.internal.processors.authentication;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test for {@link IgniteAuthenticationProcessor}.
 */
public class AuthenticationProcessorPersistEnabledTest extends AuthenticationProcessorSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cfg.isClientMode() == null) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQ() throws Exception {
        final IgniteInternalFuture restartFut = restartCoordinator();

        AuthorizationContext.context(actxDflt);

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                AuthorizationContext.context(actxDflt);
                String user = "test" + usrCnt.getAndIncrement();

                try {
                    while (!restartFut.isDone()) {
                        grid(CLI_NODE).context().authentication().addUser(user, "passwd_" + user);

                        grid(CLI_NODE).context().authentication().updateUser(user, "new_passwd_" + user);

                        grid(CLI_NODE).context().authentication().removeUser(user);
                    }
                }
                catch (Exception e) {
                    U.error(log, "Unexpected exception on concurrent add/remove", e);
                    fail();
                }
            }
        }, 1, "user-op");

        restartFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void _testUserPersistence() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            for (int i = 0; i < NODES_COUNT; ++i)
                grid(i).context().authentication().addUser("test" + i , "passwd" + i);

            grid(CLI_NODE).context().authentication().updateUser("ignite", "new_passwd");

            stopAllGrids();

            System.out.println("+++ RESTART");

            startGrids(NODES_COUNT);

            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int usrIdx = 0; usrIdx < NODES_COUNT; ++usrIdx) {
                    AuthorizationContext actx = grid(i).context().authentication()
                        .authenticate("test" + usrIdx, "passwd" + usrIdx);

                    assertNotNull(actx);
                    assertEquals("test" + usrIdx, actx.userName());
                }

                AuthorizationContext actx = grid(i).context().authentication()
                    .authenticate("ignite", "new_passwd");

                assertNotNull(actx);
                assertEquals("ignite", actx.userName());
            }
        }
        finally {
            AuthorizationContext.clear();
        }
    }

}
