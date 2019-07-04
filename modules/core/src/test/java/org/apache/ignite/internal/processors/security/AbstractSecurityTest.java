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

package org.apache.ignite.internal.processors.security;

import java.security.Permissions;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.impl.PermissionsBuilder;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Common class for security tests.
 */
public class AbstractSecurityTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param instanceName Instance name.
     * @param pluginProv Security plugin provider.
     */
    protected IgniteConfiguration getConfiguration(String instanceName,
        AbstractTestSecurityPluginProvider pluginProv) throws Exception {

        return getConfiguration(instanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setPluginProviders(pluginProv);
    }

    /** */
    protected IgniteEx startGridAllowAll(String login) throws Exception {
        return startGrid(login, PermissionsBuilder.createAllowAll(), false);
    }

    /** */
    protected IgniteEx startClientAllowAll(String login) throws Exception {
        return startGrid(login, PermissionsBuilder.createAllowAll(), true);
    }

    protected IgniteEx startGrid(String login, Permissions permissions, boolean isClient) throws Exception {
        return startGrid(getConfiguration(login, new TestSecurityPluginProvider(login, "", permissions))
            .setClientMode(isClient));
    }
}
