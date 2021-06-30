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

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;

/** */
public class AbstractDdlIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static final String CLIENT_NODE_NAME = "client";

    /** */
    protected static final String DATA_REGION_NAME = "test_data_region";

    /** */
    protected IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);

        client = startClientGrid(CLIENT_NODE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(
                new SqlConfiguration().setSqlSchemas("MY_SCHEMA")
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(new DataRegionConfiguration().setName(DATA_REGION_NAME))
            );
    }

    /** */
    @Before
    public void init() {
        client = grid(CLIENT_NODE_NAME);
    }

    /** */
    @After
    public void cleanUp() {
        client.destroyCaches(client.cacheNames());
    }

    /** */
    protected List<List<?>> executeSql(String sql) {
        List<FieldsQueryCursor<List<?>>> cur = queryProcessor().query(null, "PUBLIC", sql);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            return srvCursor.getAll();
        }
    }

    /** */
    private CalciteQueryProcessor queryProcessor() {
        return Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);
    }
}
