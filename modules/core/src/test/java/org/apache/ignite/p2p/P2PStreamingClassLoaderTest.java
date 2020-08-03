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

package org.apache.ignite.p2p;

import java.lang.reflect.Constructor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/** */
@GridCommonTest(group = "P2P")
public class P2PStreamingClassLoaderTest extends GridCommonAbstractTest {
    /** */
    private static final String ENTRY_PROCESSOR_CLASS_NAME = "org.apache.ignite.tests.p2p.NoopCacheEntryProcessor";

    /** */
    private static final String CACHE_NAME = "cache";

    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @throws Exception if error occur
     */
    @SuppressWarnings("unchecked")
    private void processTest() throws Exception {
        try {
            startGrid("server");
            Ignite client = startClientGrid("client");

            ClassLoader ldr = getExternalClassLoader();

            Class<?> epCls = ldr.loadClass(ENTRY_PROCESSOR_CLASS_NAME);

            Constructor<?> epCtr = epCls.getConstructor();

            CacheEntryProcessor ep = (CacheEntryProcessor)epCtr.newInstance();

            IgniteCache<Integer, String> cache = client.createCache(CACHE_NAME);

            try (IgniteDataStreamer<Integer, String> streamer = client.dataStreamer(CACHE_NAME)) {
                streamer.receiver(StreamTransformer.from(ep));

                streamer.addData(1, "1");
            }

            assertEquals("1", cache.get(1));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testPrivateMode() throws Exception {
        depMode = DeploymentMode.PRIVATE;

        processTest();
    }

    /**
     * Test {@link DeploymentMode#CONTINUOUS} mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest();
    }
}
