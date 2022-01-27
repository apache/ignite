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

package org.apache.ignite.platform;

import java.security.Permissions;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.GridAbstractTest;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Ignite start/stop utils.
 */
public class PlatformStartIgniteUtils {
    /**
     * Starts an Ignite instance with test security plugin provider.
     *
     * @param name Ignite instance name.
     * @throws IgniteException Exception.
     */
    public static void startWithSecurity(String name) throws IgniteException {
        TestSecurityPluginProvider securityPluginProvider = new TestSecurityPluginProvider(
                "login1",
                "pass1",
                ALLOW_ALL,
                false,
                new TestSecurityData("CLIENT", "pass1",
                        SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                                .appendCachePermissions("DEFAULT_CACHE", CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                                .appendCachePermissions("FORBIDDEN_CACHE")
                                .build(), new Permissions())
        );

        IgniteConfiguration cfg = new IgniteConfiguration()
                .setPluginProviders(securityPluginProvider)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(GridAbstractTest.LOCAL_IP_FINDER))
                .setLocalHost("127.0.0.1")
                .setIgniteInstanceName(name);

        Ignition.start(cfg);
    }

    /**
     * Stops an Ignite instance with the specified name.
     *
     * @param name Ignite instance name.
     */
    public static void stop(String name) {
        Ignition.stop(name, true);
    }
}
