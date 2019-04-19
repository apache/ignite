/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Check that cluster survives after destroy caches abruptly with disabled checkpoints.
 */
@RunWith(JUnit4.class)
public class IgnitePdsDestroyCacheWithoutCheckpointsTest extends IgnitePdsDestroyCacheAbstractTest {
    /**
     * {@inheritDoc}
     * @returns {@code true} in order to be able to kill nodes when checkpointer thread hangs.
     */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * Test destroy caches with disabled checkpoints.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyCachesAbruptlyWithoutCheckpoints() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        disableCheckpoints();

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Test destroy group caches with disabled checkpoints.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyGroupCachesAbruptlyWithoutCheckpoints() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        disableCheckpoints();

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Disable checkpoints on nodes.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void disableCheckpoints() throws IgniteCheckedException {
        for (Ignite ignite : G.allGrids()) {
            assert !ignite.cluster().localNode().isClient();

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                    .cache().context().database();

            dbMgr.enableCheckpoints(false).get();
        }
    }
}
