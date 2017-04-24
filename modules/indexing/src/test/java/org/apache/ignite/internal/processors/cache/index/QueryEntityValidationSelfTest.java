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

package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * Tests for query entity validation.
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class QueryEntityValidationSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test null value type.
     *
     * @throws Exception If failed.
     */
    public void testNullValueType() throws Exception {
        final CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");

        ccfg.setQueryEntities(Collections.singleton(entity));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).createCache(ccfg);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }
}
