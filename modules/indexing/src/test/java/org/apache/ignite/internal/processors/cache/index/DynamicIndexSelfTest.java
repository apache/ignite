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

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * Tests for dynamic index creation.
 */
public class DynamicIndexSelfTest extends GridCommonAbstractTest {
    /** Cache. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test simple index create.
     *
     * @throws Exception If failed.
     */
    public void testSimpleCreate() throws Exception {
        IgniteEx node = grid(0);

        node.getOrCreateCache(cacheConfiguration());

        GridCacheProcessor cacheProc = node.context().cache();

        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();

        idxFields.put("str", true);

        QueryIndex idx = new QueryIndex().setName("my_idx").setFields(idxFields);

        cacheProc.dynamicIndexCreate(CACHE_NAME, ValueClass.class.getSimpleName(), idx, false).get();

        Collection<GridQueryTypeDescriptor> descs = node.context().query().types(CACHE_NAME);

        System.out.println(descs);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration<KeyClass, ValueClass>()
            .setName(CACHE_NAME)
            .setIndexedTypes(KeyClass.class, ValueClass.class);
    }

    /**
     * Key class.
     */
    private static class KeyClass {
        /** ID. */
        @QuerySqlField
        private long id;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        public KeyClass(long id) {
            this.id = id;
        }

        /**
         * @return ID.
         */
        public long id() {
            return id;
        }
    }

    /**
     * Key class.
     */
    private static class ValueClass {
        /** String value. */
        @QuerySqlField
        private String str;

        /**
         * Constructor.
         *
         * @param str String value.
         */
        public ValueClass(String str) {
            this.str = str;
        }

        /**
         * @return String value.
         */
        public String stringValue() {
            return str;
        }
    }
}
