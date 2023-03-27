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
 *
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.commons.codec.binary.Hex;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryArray;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_BINARY_ARRAYS;
import static org.apache.ignite.client.Config.SERVER;

/**
 * Checks that sql operation works by arrays.
 */
public class ArrayIndexTest extends AbstractIndexingCommonTest {
    /**
     * @throws Exception if fails.
     */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if fails.
     */
    @Before
    public void setUp() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setQueryEntities(Collections.singleton(
                    new QueryEntity()
                        .setKeyType(byte[].class.getName())
                        .setValueType(Integer.class.getName())
                ))
        );

        return cfg;
    }

    /**
     *
     */
    @Test
    public void shouldSelectAllRows() throws Exception {
        IgniteEx ex = startGrid(0);

        ex.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ex.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(new byte[] {8, 9, 10}, 3);
        cache.put(new byte[] {1, 2, 3}, 1);
        cache.put(new byte[] {5, 6, 7}, 2);

        List<List<?>> sorted = cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll();

        assertEquals(3, sorted.size());

        List<?> first = sorted.get(0);
        assertTrue(Objects.deepEquals(first.get(0), new byte[] {1, 2, 3}));
        assertTrue(Objects.deepEquals(first.get(1), 1));

        List<?> second = sorted.get(1);
        assertTrue(Objects.deepEquals(second.get(0), new byte[] {5, 6, 7}));
        assertTrue(Objects.deepEquals(second.get(1), 2));

        List<?> third = sorted.get(2);
        assertTrue(Objects.deepEquals(third.get(0), new byte[] {8, 9, 10}));
        assertTrue(Objects.deepEquals(third.get(1), 3));
    }

    /**
     *
     */
    @Test
    public void shouldSelectParticularValue() throws Exception {
        IgniteEx ex = startGrid(0);

        ex.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ex.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(new byte[] {1, 2, 3}, 1);
        cache.put(new byte[] {5, 6, 7}, 2);
        cache.put(new byte[] {8, 9, 10}, 3);

        List<List<?>> res = cache.query(new SqlFieldsQuery("select _key, _val from Integer where _key = ?")
            .setArgs(new byte[] {5, 6, 7}))
            .getAll();

        assertEquals(1, res.size());

        List<?> row = res.get(0);
        assertTrue(Objects.deepEquals(row.get(0), new byte[] {5, 6, 7}));
        assertTrue(Objects.deepEquals(row.get(1), 2));
    }

    /**
     *
     */
    @Test
    public void shouldCreateTableWithBinaryKey() throws Exception {
        byte[] key = {1, 2, 3, 4};

        IgniteEx ex = startGrid(0);

        ex.cluster().state(ClusterState.ACTIVE);

        executeSql(ex, "CREATE TABLE Binary_Entries (key binary(16) not null, val binary(16), PRIMARY KEY(key))");

        executeSql(ex, "INSERT INTO Binary_Entries(key, val) VALUES (x'" + Hex.encodeHexString(key) + "', x'01020304')");

        assertEquals(ex.cache("SQL_PUBLIC_BINARY_ENTRIES").size(), 1);
        assertTrue(ex.cache("SQL_PUBLIC_BINARY_ENTRIES").containsKey(key));

    }

    /** */
    @Test
    public void shouldSupportTableExpressions() throws Exception {
        checkTableExpression(false);
    }

    /** */
    @Test
    public void shouldSupportTableExpressionsWithBinaryArrays() throws Exception {
        checkTableExpression(true);
    }

    /** */
    private void checkTableExpression(boolean useTypedArrays) throws Exception {
        System.setProperty(IGNITE_USE_BINARY_ARRAYS, Boolean.toString(useTypedArrays));
        BinaryArray.initUseBinaryArrays();

        try (IgniteEx ex = startGrid(0);
             IgniteEx cli = startClientGrid(1);
             IgniteClient thinCli = Ignition.startClient(new ClientConfiguration().setAddresses(SERVER))) {

            ex.cluster().state(ClusterState.ACTIVE);

            executeSql(cli, "CREATE TABLE T1 (id int not null, name varchar(1), PRIMARY KEY(id))");

            String insertQry = "INSERT INTO T1(id, name) VALUES (?, ?)";

            executeSql(cli, insertQry, 1, "A");
            executeSql(cli, insertQry, 2, "B");
            executeSql(cli, insertQry, 3, "C");

            String select = "SELECT T1.ID, T1.NAME FROM T1 INNER JOIN TABLE (id2 int = ?) T2 on (T1.id = T2.id2) ORDER BY id";
            Object arg = new Integer[] {1, 2};

            Consumer<List<List<?>>> checker = res -> {
                assertNotNull(res);

                assertEquals(2, res.size());

                assertEquals(1, res.get(0).get(0));
                assertEquals("A", res.get(0).get(1));
                assertEquals(2, res.get(1).get(0));
                assertEquals("B", res.get(1).get(1));
            };

            checker.accept(executeSql(ex, select, arg));
            checker.accept(executeSql(cli, select, arg));
            checker.accept(thinCli.query(new SqlFieldsQuery(select).setArgs(arg)).getAll());
        }
        finally {
            System.clearProperty(IGNITE_USE_BINARY_ARRAYS);
            BinaryArray.initUseBinaryArrays();
        }
    }

    /**
     *
     */
    private List<List<?>> executeSql(IgniteEx node, String sqlText, Object... args) throws Exception {
        GridQueryProcessor qryProc = node.context().query();

        return qryProc.querySqlFields(new SqlFieldsQuery(sqlText).setArgs(args), true).getAll();
    }
}
