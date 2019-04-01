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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.property.QueryClassProperty;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * A test for {@link QueryEntity} initialization with incorrect query entity configuration if {@link QueryClassProperty}
 * is used.
 */
public class IncorrectQueryEntityForClassPropertyTest extends GridCommonAbstractTest {
    /**
     * Cretates incorrect configuration to test {@link QueryUtils#buildClassProperty(Class, Class, String, Class, Map,
     * boolean, CacheObjectContext)}. Non-binary marshaller is required to do this.
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new OptimizedMarshaller());

        CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

        // Use string keyType to avoid checks on the client side.
        QueryEntity queryEntity = new QueryEntity("SomeKeyClass", "java.lang.Long");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("someFieldName", Object.class.getName());

        queryEntity.setFields(fields);

        Set<String> keyFields = new HashSet<>();

        keyFields.add("someFieldName");

        queryEntity.setKeyFields(keyFields);

        dfltCacheCfg.setQueryEntities(F.asList(queryEntity));

        cfg.setCacheConfiguration(dfltCacheCfg);

        return cfg;
    }

    /**
     * Grid must be stopped with property initialization exception.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectQueryField() throws Exception {
        String expMsg = QueryUtils.propertyInitializationExceptionMessage(
            Object.class, Long.class, "someFieldName", Object.class);

        GridTestUtils.assertThrows(log(), this::startGrid, IgniteCheckedException.class, expMsg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
