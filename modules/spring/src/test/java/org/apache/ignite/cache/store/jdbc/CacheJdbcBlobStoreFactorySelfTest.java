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

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.h2.jdbcx.*;

/**
 * Test for Cache jdbc blob store factory.
 */
public class CacheJdbcBlobStoreFactorySelfTest extends GridCommonAbstractTest {
    public void testXmlConfiguration() throws Exception {
        try (Ignite ignite = Ignition.start("modules/spring/src/test/config/store-cache.xml")) {
            IgniteCache<Integer, String> cache = ignite.cache("test");

            CacheJdbcBlobStore store = (CacheJdbcBlobStore) cache.getConfiguration(CacheConfiguration.class).
                getCacheStoreFactory().create();

            assertEquals("GridGain", GridTestUtils.getFieldValue(store, CacheJdbcBlobStore.class, "user"));

            assertEquals(JdbcDataSource.class,
                GridTestUtils.getFieldValue(store, CacheJdbcBlobStore.class, "dataSrc").getClass());
        }
    }
}
