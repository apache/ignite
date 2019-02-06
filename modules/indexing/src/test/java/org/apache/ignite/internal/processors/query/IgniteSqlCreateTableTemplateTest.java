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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Ensures that SQL queries work for tables created dynamically based on a template.
 */
public class IgniteSqlCreateTableTemplateTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override
    protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration configuration = super.getConfiguration();
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName("TEST_TEMPLATE*");
        configuration.setCacheConfiguration(cacheConfiguration);
        return configuration;
    }


    /**
     * Tests select statement works on a table with BinaryObject as a primary key.
     */
    @Test
    public void testSelectForTableWithDataInsertedWithKeyValueAPI() throws Exception {
        Ignite ignite = startGrid(getConfiguration());
        IgniteCache cache = ignite.getOrCreateCache("test");

        cache.query(new SqlFieldsQuery(
                "CREATE TABLE IF NOT EXISTS TEST(\n" +
                "  TEST_ID                INT        NOT NULL,\n" +
                "  TEST_FIELD             VARCHAR2(100),\n" +
                "  PRIMARY KEY (TEST_ID)\n" +
                ") with \"TEMPLATE=TEST_TEMPLATE,KEY_TYPE=TEST_KEY ,CACHE_NAME=TEST_CACHE , VALUE_TYPE=TEST_VALUE,ATOMICITY=TRANSACTIONAL\";").setSchema("PUBLIC"));

        BinaryObjectBuilder keyBuilder = ignite.binary().builder("TEST_KEY");
        keyBuilder.setField("TEST_ID", 1);

        BinaryObjectBuilder valueBuilder = ignite.binary().builder("TEST_VALUE");
        valueBuilder.setField("TEST_FIELD", "test");

        ignite.cache("TEST_CACHE").withKeepBinary().put(keyBuilder.build(), valueBuilder.build());

        assertEquals(1, ignite.cache("TEST_CACHE").query(new SqlFieldsQuery("Select TEST_FIELD from TEST where TEST_ID = 1")).getAll().size());
    }
}