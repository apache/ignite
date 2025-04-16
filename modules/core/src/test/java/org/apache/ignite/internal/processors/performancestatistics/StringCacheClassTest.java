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

package org.apache.ignite.internal.processors.performancestatistics;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;

/** Test {@link StringCache}. */
public class StringCacheClassTest extends GridCommonAbstractTest {
    /** Test value of {@link IgniteSystemProperties#IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD}. */
    private static final int TEST_CACHED_STRINGS_THRESHOLD = 3;

    /** */
    @Test
    public void testCache() {
        StringCache cache = new StringCache();

        assertFalse(cache.cacheIfPossible("A"));

        assertTrue(cache.cacheIfPossible("A"));
        assertTrue(cache.cacheIfPossible("A"));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, value = "" + TEST_CACHED_STRINGS_THRESHOLD)
    public void testThreshold() {
        StringCache cache = new StringCache();

        assertFalse(cache.cacheIfPossible("1"));
        assertTrue(cache.cacheIfPossible("1"));

        assertFalse(cache.cacheIfPossible("2"));
        assertTrue(cache.cacheIfPossible("2"));

        assertFalse(cache.cacheIfPossible("3"));
        assertFalse(cache.cacheIfPossible("3"));
    }
}
