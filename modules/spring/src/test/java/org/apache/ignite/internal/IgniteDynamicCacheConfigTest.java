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

package org.apache.ignite.internal;


import org.apache.ignite.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Test for dynamic cache start from config file.
 */
public class IgniteDynamicCacheConfigTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicCacheStartFromConfig() throws Exception {
        IgniteCache cache = ignite(0).createCache("modules/core/src/test/config/cache.xml");

        assertEquals("TestDynamicCache", cache.getName());

        IgniteCache cache1 = ignite(0).getOrCreateCache("modules/core/src/test/config/cache.xml");

        assertEquals(cache, cache1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicNearCacheStartFromConfig() throws Exception {
        IgniteCache cache1 = ignite(0).getOrCreateCache("modules/core/src/test/config/cache.xml",
            "modules/core/src/test/config/cache.xml");

        assertEquals("TestDynamicCache", cache1.getName());
    }
}
