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

package org.apache.ignite.internal.processors.cache.context;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 *
 */
public abstract class IgniteCacheAbstractExecutionContextTest extends IgniteCacheAbstractTest {
    /** */
    public static final String TEST_VALUE = "org.apache.ignite.tests.p2p.CacheDeploymentTestValue";

    /** */
    public static final int ITER_CNT = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClassLoader(new UsersClassLoader());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheConfiguration = super.cacheConfiguration(gridName);

        cacheConfiguration.setBackups(1);

        return cacheConfiguration;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUsersClassLoader() throws Exception {
        UsersClassLoader testClassLdr = (UsersClassLoader)grid(0).configuration().getClassLoader();

        Object val = testClassLdr.loadClass(TEST_VALUE).newInstance();

        IgniteCache<Object, Object> jcache = grid(0).cache(null);

        for (int i = 0; i < ITER_CNT; i++)
            jcache.put(i, val);

        for (int i = 0; i < ITER_CNT; i++) {
            int idx = i % gridCount();

            // Check that entry was loaded by user's classloader.
            if (idx == 0)
                assertEquals(testClassLdr, jcache.get(i).getClass().getClassLoader());
            else
                assertEquals(grid(idx).configuration().getClassLoader(),
                    grid(idx).cache(null).get(i).getClass().getClassLoader());
        }
    }

    /**
     *
     */
    private static class UsersClassLoader extends GridTestExternalClassLoader {
        /**
         * @throws MalformedURLException If failed
         */
        public UsersClassLoader() throws MalformedURLException {
            super(new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))});
        }
    }
}
