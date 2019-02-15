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

package org.apache.ignite.internal.encryption;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@RunWith(JUnit4.class)
public class EncryptedCachePreconfiguredRestartTest extends EncryptedCacheRestartTest {
    /** */
    private boolean differentCachesOnNodes;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testDifferentPreconfiguredCachesOnNodes() throws Exception {
        differentCachesOnNodes = true;

        super.testCreateEncryptedCache();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testCreateEncryptedCache() throws Exception {
        differentCachesOnNodes = false;

        super.testCreateEncryptedCache();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        String cacheName = ENCRYPTED_CACHE + (differentCachesOnNodes ? "." + igniteInstanceName : "");

        CacheConfiguration ccfg = new CacheConfiguration(cacheName)
            .setEncryptionEnabled(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Cache name.
     */
    @NotNull @Override protected String cacheName() {
        return ENCRYPTED_CACHE + (differentCachesOnNodes ? "." + GRID_1 : "");
    }

    /**
     * Creates encrypted cache.
     */
    @Override protected void createEncryptedCache(IgniteEx grid0, IgniteEx grid1, String cacheName, String groupName) {
        IgniteCache<Long, String> cache = grid0.cache(cacheName());

        for (long i=0; i<100; i++)
            cache.put(i, "" + i);
    }
}
