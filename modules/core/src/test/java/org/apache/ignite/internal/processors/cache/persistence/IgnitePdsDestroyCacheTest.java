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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;

/**
 * Test correct clean up cache configuration data after destroying cache.
 */
public class IgnitePdsDestroyCacheTest extends IgnitePdsDestroyCacheAbstractTest {
    /**
     *  Test destroy non grouped caches.
     *
     *  @throws Exception If failed.
     */
    public void testDestroyCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     *  Test destroy grouped caches.
     *
     *  @throws Exception If failed.
     */
    public void testDestroyGroupCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     * Test destroy caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }
    
    /**
     * Test destroy group caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyGroupCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }
}
