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
package org.apache.ignite.internal.processors.query.calcite.thin;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests proper exception is thrown when multiline expressions are executed on thin client on both engines.
 */
@RunWith(Parameterized.class)
public class MultiLineQueryTest extends GridCommonAbstractTest {
    /** */
    private static final String H2_ENGINE = IndexingQueryEngineConfiguration.ENGINE_NAME;

    /** */
    private static final String CALCITE_ENGINE = CalciteQueryEngineConfiguration.ENGINE_NAME;

    /** */
    private IgniteClient cli;

    /** */
    @Parameterized.Parameter
    public String queryEngine;

    /** */
    @Parameterized.Parameters(name = "engine={0}")
    public static List<Object> parameters() {
        return F.asList(H2_ENGINE, CALCITE_ENGINE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(
                new IndexingQueryEngineConfiguration(),
                new CalciteQueryEngineConfiguration()
            )
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(1);

        cli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"));

        execute("create table if not exists test(id int primary key, val varchar)");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (cli != null) {
            cli.close();

            cli = null;
        }

        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testMultilineThrowsProperException() {
        assertThrows(log(), () -> {
            execute("insert /* QUERY_ENGINE('" + queryEngine + "') */ into test (id, val) values (1, 'hello');" +
                "select /* QUERY_ENGINE('" + queryEngine + "') */ val from test where id = 1");
        }, IgniteException.class, "Multiple statements queries are not supported");
    }

    /** */
    private List<List<?>> execute(String sql) {
        return cli.query(new SqlFieldsQuery(sql)).getAll();
    }
}
