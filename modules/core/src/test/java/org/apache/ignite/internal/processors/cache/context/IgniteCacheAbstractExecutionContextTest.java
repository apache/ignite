/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public abstract class IgniteCacheAbstractExecutionContextTest extends IgniteCacheAbstractTest {
    /** */
    public static final String TEST_VALUE = "org.apache.ignite.tests.p2p.CacheDeploymentTestValue";

    /** */
    public static final int ITER_CNT = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClassLoader(new UsersClassLoader());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cacheConfiguration = super.cacheConfiguration(igniteInstanceName);

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
    @Test
    public void testUsersClassLoader() throws Exception {
        UsersClassLoader testClassLdr = (UsersClassLoader)grid(0).configuration().getClassLoader();

        Object val = testClassLdr.loadClass(TEST_VALUE).newInstance();

        IgniteCache<Object, Object> jcache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ITER_CNT; i++)
            jcache.put(i, val);

        for (int i = 0; i < ITER_CNT; i++) {
            int idx = i % gridCount();

            // Check that entry was loaded by user's classloader.
            if (idx == 0)
                assertEquals(testClassLdr, jcache.get(i).getClass().getClassLoader());
            else
                assertEquals(grid(idx).configuration().getClassLoader(),
                    grid(idx).cache(DEFAULT_CACHE_NAME).get(i).getClass().getClassLoader());
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
