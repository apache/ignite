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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.map;
import static org.apache.ignite.testframework.GridTestUtils.hasSize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** */
public class IndexDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /**
     * Creates index with two columns.
     */
    @Test
    public void createIndexSimpleCase() {
        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(new CacheConfiguration<Integer, Integer>("my_cache")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Integer.class).setTableName("my_table")))
        );

        executeSql("create index my_index on my_table(_val)");

        CacheConfiguration<?, ?> ccfg = client.cachex("my_cache").configuration();

        assertThat(ccfg.getQueryEntities(), hasSize(1));

        QueryEntity ent = ccfg.getQueryEntities().iterator().next();

        Collection<QueryIndex> idxs = ent.getIndexes();

        // TODO
    }
}
