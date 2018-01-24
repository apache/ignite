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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * A test for {@link CacheTypeMetadata} initialization with incorrect query field name
 */
public class IncorrectCacheTypeMetadataTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

        CacheTypeMetadata cacheTypeMetadata = new CacheTypeMetadata();

        Map<String, Class<?>> qryFieldsMap = new HashMap<>();

        qryFieldsMap.put("exceptionOid", Object.class);

        cacheTypeMetadata.setQueryFields(qryFieldsMap);
        cacheTypeMetadata.setValueType(Object.class);

        dfltCacheCfg.setTypeMetadata(Collections.singleton(cacheTypeMetadata));

        cfg.setCacheConfiguration(dfltCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Grid must be stopped with property initialization exception.
     *
     * @throws Exception If failed.
     */
    public void testIncorrectQueryField() throws Exception {
        try {
            startGrid();
        }
        catch (Exception exception) {
            if (!exception.getMessage().contains(
                GridQueryProcessor.propertyInitializationExceptionMessage(
                    Object.class, Object.class, "exceptionOid", Object.class)))
                fail("property initialization exception must be thrown, but got " + exception.getMessage());
        }
    }
}
