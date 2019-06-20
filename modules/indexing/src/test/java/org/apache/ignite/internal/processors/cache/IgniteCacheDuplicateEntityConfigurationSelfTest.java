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
package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheDuplicateEntityConfigurationSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClassDuplicatesQueryEntity() throws Exception {
        String cacheName = "duplicate";

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setIndexedTypes(Integer.class, Person.class);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("name", String.class.getName());

        entity.setFields(fields);

        ccfg.setQueryEntities(Arrays.asList(entity));

        try {
            ignite(0).getOrCreateCache(ccfg);
        }
        finally {
            ignite(0).destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClassDuplicatesQueryReverse() throws Exception {
        String cacheName = "duplicate";

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("name", String.class.getName());

        entity.setFields(fields);

        ccfg.setQueryEntities(Arrays.asList(entity));

        ccfg.setIndexedTypes(Integer.class, Person.class);

        try {
            ignite(0).getOrCreateCache(ccfg);
        }
        finally {
            ignite(0).destroyCache(cacheName);
        }
    }

    private static class Person {
        @QuerySqlField
        private String name;
    }
}
