/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.Test;

import static java.util.Collections.singletonList;

/** */
public class SqlFieldsQueryUsageTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);

        client = startClientGrid();
    }

    /** */
    @AfterClass
    public static void tearDown() {
        G.stopAll(false);
    }

    /**
     * Temporary redirects create|drop|alter commands into h2 engine.
     */
    @Test
    public void testUseH2Functionality() {
        execute(grid(0), "CREATE TABLE IF NOT EXISTS Person(\"id\" INT, PRIMARY KEY(\"id\"), \"name\" VARCHAR)");

        execute(grid(0), "alter table Person add column age int");
        execute(grid(0), "drop table Person");
    }

    /** */
    @Test
    public void createCacheOnSrvCallOnCli() throws InterruptedException {
        QueryEntity projEntity = new QueryEntity();
        projEntity.setKeyType(Integer.class.getName());
        projEntity.setKeyFieldName("id");
        projEntity.setValueType(Integer.class.getName());
        projEntity.addQueryField("id", Integer.class.getName(), null);

        projEntity.setTableName("Developer");

        CacheConfiguration<Integer, Integer> projCfg =
            new CacheConfiguration<Integer, Integer>(projEntity.getTableName())
                .setQueryEntities(singletonList(projEntity))
                .setSqlSchema("PUBLIC");

        IgniteCache<Integer, Integer> devCache = grid(0).createCache(projCfg);

        awaitPartitionMapExchange();

        assertFalse(grid(0).configuration().isClientMode());

        devCache.put(1, 2);

        assertEquals(1, execute(client, "SELECT * FROM Developer").size());;
    }

    /**
     * Execute SQL statement on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    protected List<List<?>> execute(IgniteEx node, String sql) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC"), true).getAll();
    }
}
