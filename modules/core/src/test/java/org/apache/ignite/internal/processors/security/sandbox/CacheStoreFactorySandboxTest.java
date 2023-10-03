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

import java.security.AccessControlException;
import java.security.Permissions;
import java.util.PropertyPermission;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;

/** */
public class CacheStoreFactorySandboxTest extends AbstractSandboxTest {
    /** */
    private static final String SRV_FORBIDDEN = "srv_forbidden";

    /** */
    private boolean staticCache;

    /** */
    private static Permissions sandboxPerm;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        if (staticCache)
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void prepareCluster() {
        Permissions perms = new Permissions();

        perms.add(new PropertyPermission(PROP_NAME, "write"));

        sandboxPerm = perms;
    }

    /** */
    @Test
    public void testStaticCacheStoreFactory() {
        staticCache = true;

        runForbiddenOperation(() ->
            startGrid(SRV_FORBIDDEN, ALL_PERMISSIONS, false),
            AccessControlException.class);

        runOperation(() -> {
            try (IgniteEx ign = startGrid(SRV_FORBIDDEN, ALL_PERMISSIONS, sandboxPerm, false)) {
                ign.cache(DEFAULT_CACHE_NAME).put(0, 0);
            }
            catch (Exception e) {
                assert false : e;
            }
        });
    }

    /** */
    @Test
    public void testDynamicCacheStoreFactory() throws Exception {
        staticCache = false;

        try (IgniteEx ign = startGrid("srv_forbidden", ALL_PERMISSIONS, false)) {
            runForbiddenOperation(() -> ign.createCache(cacheConfiguration()), AccessControlException.class);
        }

        try (IgniteEx ign = startGrid("srv_forbidden", ALL_PERMISSIONS, sandboxPerm, false)) {
            runOperation(() -> {
                try {
                    IgniteCache<Integer, Integer> c = ign.createCache(cacheConfiguration());

                    c.put(0, 0);
                }
                catch (CacheException e) {
                    assert false : e;
                }
            });
        }
    }

    /** */
    private CacheConfiguration<Integer, Integer> cacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheStoreFactory(() -> {
                controlAction();

                return null;
            });
    }
}
