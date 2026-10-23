/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that a warning is printed when {@link AffinityKeyMapped} annotation is placed
 * on a field of a cache value class, where it has no effect.
 */
public class AffinityKeyMappedOnValueWarningSelfTest extends GridCommonAbstractTest {
    /** Distinctive part of the expected warning message. */
    private static final String WARN_MSG =
        "is ignored, because the class is used as a cache value";

    /** */
    private static final LogListener LSNR = LogListener.matches(WARN_MSG).times(1).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        testLog.registerListener(LSNR);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        LSNR.reset();
    }

    /**
     * A warning must be printed exactly once per type when a cache value class
     * contains a field annotated with {@link AffinityKeyMapped}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWarningOnAnnotatedValue() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<TestKey, WrongValue> cache = ignite.createCache(DEFAULT_CACHE_NAME);

        cache.put(new TestKey(1), new WrongValue(1, "Denis"));
        cache.put(new TestKey(2), new WrongValue(2, "Ivan"));

        assertTrue("Warning about @AffinityKeyMapped on a cache value must be printed exactly once per type",
            LSNR.check());
    }

    /**
     * No warning must be printed when {@link AffinityKeyMapped} is placed on a key class.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoWarningOnAnnotatedKey() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<CorrectKey, PlainValue> cache = ignite.createCache(DEFAULT_CACHE_NAME);

        cache.put(new CorrectKey(1, "Denis"), new PlainValue("Denis"));

        assertFalse("No warning expected when @AffinityKeyMapped is placed on a key",
            LSNR.check());
    }

    /** */
    private static class TestKey {
        /** */
        private int id;

        /** */
        TestKey(int id) {
            this.id = id;
        }
    }

    /** Value with a misplaced annotation. */
    private static class WrongValue {
        /** */
        @AffinityKeyMapped
        private int departmentId;

        /** */
        private String name;

        /** */
        WrongValue(int departmentId, String name) {
            this.departmentId = departmentId;
            this.name = name;
        }
    }

    /** Key with a correctly placed annotation. */
    private static class CorrectKey {
        /** */
        @AffinityKeyMapped
        private int departmentId;

        /** */
        private String name;

        /** */
        CorrectKey(int departmentId, String name) {
            this.departmentId = departmentId;
            this.name = name;
        }
    }

    /** */
    private static class PlainValue {
        /** */
        private String name;

        /** */
        PlainValue(String name) {
            this.name = name;
        }
    }
}
