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

package org.apache.ignite.internal.processors.cache.binary;

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Cache EntryProcessor + Deployment.
 */
@RunWith(JUnit4.class)
public class GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest
    extends GridCacheAtomicEntryProcessorDeploymentSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteCache getCache() {
        return grid(1).cache(DEFAULT_CACHE_NAME).withKeepBinary();
    }

    /** {@inheritDoc} */
    @Override protected String getEntryProcessor() {
        return "org.apache.ignite.tests.p2p.CacheDeploymentBinaryObjectEntryProcessor";
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetDeployment() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        doTestGet(false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetDeployment2() throws Exception {
        depMode = DeploymentMode.SHARED;

        doTestGet(false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetDeploymentWithKeepBinary() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        doTestGet(true);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetDeployment2WithKeepBinary() throws Exception {
        depMode = DeploymentMode.SHARED;

        doTestGet(true);
    }

    /**
     * @throws Exception Exception.
     */
    private void doTestGet(boolean withKeepBinary) throws Exception {
        try {
            clientMode = false;
            startGrid(0);

            clientMode = true;
            startGrid(1);

            Class valCls = grid(1).configuration().getClassLoader().loadClass(TEST_VALUE);

            assertTrue(grid(1).configuration().isClientMode());
            assertFalse(grid(0).configuration().isClientMode());

            IgniteCache cache1 = grid(1).cache(DEFAULT_CACHE_NAME);
            IgniteCache cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

            if (withKeepBinary) {
                cache1 = cache1.withKeepBinary();
                cache0 = cache0.withKeepBinary();
            }

            cache1.put("key", valCls.newInstance());

            if (withKeepBinary) {
                BinaryObject obj = (BinaryObject)(cache0.get("key"));

                try {
                    obj.deserialize();

                    fail("Exception did not happened.");
                }
                catch (BinaryInvalidTypeException ignored) {
                    // No-op.
                }
            }
            else
                try {
                    cache0.get("key");

                    fail("Exception did not happened.");
                }
                catch (CacheException ex) {
                    assertTrue(X.hasCause(ex, BinaryInvalidTypeException.class));
                }
        }
        finally {
            stopAllGrids();
        }
    }
}
