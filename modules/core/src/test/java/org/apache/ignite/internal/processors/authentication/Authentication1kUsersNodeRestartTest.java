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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link IgniteAuthenticationProcessor} on unstable topology.
 */
public class Authentication1kUsersNodeRestartTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(200L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }
    /**
     * @throws Exception If failed.
     */
    public void test1kUsersNodeRestartServer() throws Exception {
        final int USERS_COUNT = 1000;

        startGrid(0);

        grid(0).cluster().active(true);

        final AuthorizationContext actxDflt = grid(0).context().authentication().authenticate(User.DFAULT_USER_NAME, "ignite");

        final AtomicInteger usrCnt = new AtomicInteger();

        AuthorizationContext.context(actxDflt);

        for (int i = 0; i < USERS_COUNT; ++i)
            grid(0).context().authentication().addUser("test" + i, "init");

        usrCnt.set(0);

        for (int i = 0; i < USERS_COUNT; ++i)
            grid(0).context().authentication().updateUser("test"  + i, "passwd_" + i);

        System.out.println("+++ STOP");

        U.sleep(1000);

        stopGrid(0);

        U.sleep(1000);

        System.out.println("+++ START");
        startGrid(0);

        AuthorizationContext actx = grid(0).context().authentication().authenticate("ignite", "ignite");
    }
}
