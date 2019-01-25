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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing names of indexes.
 */
@RunWith(JUnit4.class)
public class IgniteIndexNamesTest extends GridCommonAbstractTest {
    /** */
    private static final String TABLE_CACHE_NAME = "FOO";

    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private boolean useJdbc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /** */
    @Before
    public void before() throws Exception {
        // Being paranoid.
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testNamesUseJava() throws Exception {
        useJdbc = false;

        check();
    }

    /** */
    @Test
    public void testNamesUseJdbc() throws Exception {
        useJdbc = true;

        check();
    }

    /** */
    private void check() throws Exception {
        startGrids(NODES_COUNT);

        grid(0).cluster().active(true);

        // Create gateway cache.
        grid(0).createCache(DEFAULT_CACHE_NAME);

        runSql("CREATE TABLE PUBLIC.FOO (A INT, B INT, C INT PRIMARY KEY) WITH \"CACHE_NAME=FOO\"");

        List<String> createIndexQueries = Arrays.asList(
            "CREATE INDEX ON PUBLIC.FOO (a)",
            "CREATE INDEX ON PUBLIC.FOO (b)",
            "CREATE INDEX ON PUBLIC.FOO (c)",
            "CREATE INDEX \"idx\" ON PUBLIC.FOO (a)",
            "CREATE INDEX \"IDX\" ON PUBLIC.FOO (a)",
            "CREATE INDEX \"iDx\" ON PUBLIC.FOO (a)",
            "CREATE INDEX \"\" ON PUBLIC.FOO (a)",
            "CREATE INDEX \" \" ON PUBLIC.FOO (a)"
        );

        for (String sql : createIndexQueries)
            runSql(sql);

        List<String> expIndexNames = Arrays.asList(
            "foo_a_asc_idx",
            "foo_b_asc_idx",
            "foo_c_asc_idx",
            "idx",
            "IDX",
            "iDx",
            "",
            " "
        );

        Collections.sort(expIndexNames);

        checkIndexes(expIndexNames);

        // Check that repeated queries fail.
        for (String sql : createIndexQueries) {
            Throwable e = GridTestUtils.assertThrows(
                log,
                () -> {
                    runSql(sql);
                    return null;
                },
                useJdbc ? SQLException.class : IgniteSQLException.class,
                "Index already exists"
            );

            int errorCode = useJdbc ? ((SQLException)e).getErrorCode() : ((IgniteSQLException)e).statusCode();

            assertEquals(IgniteQueryErrorCode.INDEX_ALREADY_EXISTS, errorCode);
        }

        checkIndexes(expIndexNames);

        // Check that indexes are the same after restart.
        stopAllGrids();

        startGrids(NODES_COUNT);

        checkIndexes(expIndexNames);
    }

    /** */
    private void checkIndexes(List<String> expIdxNames) {
        for (int i = 0; i < NODES_COUNT; i++) {
            IgniteEx igniteEx = grid(i);

            List<String> indexNames = igniteEx.context().cache().cacheDescriptors().get(TABLE_CACHE_NAME)
                .schema()
                .entities()
                .stream()
                .flatMap(entity -> entity.getIndexes().stream())
                .map(QueryIndex::getName)
                .sorted((o1, o2) -> {
                    if (o1 == null)
                        return o2 == null ? 0 : -1;
                    else if (o2 == null)
                        return 1;
                    else
                        return o1.compareTo(o2);
                })
                .collect(Collectors.toList());

            assertEqualsCollections(expIdxNames, indexNames);
        }
    }

    /** */
    private void runSql(String sql) throws SQLException {
        if (useJdbc) {
            try (
                Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800");
                Statement statement = conn.createStatement()
            ) {
                statement.execute(sql);
            }
        }
        else
            grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)).getAll();
    }
}
