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
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Cache interceptor related tests. */
@RunWith(Parameterized.class)
public class CacheWithInterceptorIntegrationTest extends GridCommonAbstractTest {
    /** Node role. */
    @Parameterized.Parameter(0)
    public boolean keepBinary;

    /** */
    @Parameterized.Parameters(name = "keepBinary={0}")
    public static Collection<?> parameters() {
        return List.of(true, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        var entity0 = new QueryEntity()
            .setTableName("Pure")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .setKeyFieldName("id")
            .setValueFieldName("name");

        var personCfg = new CacheConfiguration<Integer, Object>("person")
            .setAtomicityMode(TRANSACTIONAL)
            .setSqlSchema("PUBLIC")
            .setInterceptor(new TestCacheInterceptor(keepBinary))
            .setQueryEntities(List.of(new QueryEntity(Integer.class, Person.class)
                .setTableName("PERSON")
                .addQueryField("ID", Integer.class.getName(), null)
                .setKeyFieldName("ID")
            ));

        var cityCfg = new CacheConfiguration<Integer, Object>("city")
            .setAtomicityMode(TRANSACTIONAL)
            .setSqlSchema("PUBLIC")
            .setInterceptor(new TestCacheInterceptor(keepBinary))
            .setQueryEntities(List.of(new QueryEntity(Integer.class, City.class)
                .setTableName("CITY")
                .addQueryField("ID", Integer.class.getName(), null)
                .setKeyFieldName("ID")
            ));

        var pureCacheCfg = new CacheConfiguration<Integer, Object>("pure")
            .setAtomicityMode(TRANSACTIONAL)
            .setSqlSchema("PUBLIC")
            .setInterceptor(new TestCacheInterceptor(false))
            .setQueryEntities(List.of(entity0));

        var calciteQryEngineCfg = new CalciteQueryEngineConfiguration().setDefault(true);

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(calciteQryEngineCfg))
            .setTransactionConfiguration(new TransactionConfiguration().setTxAwareQueriesEnabled(true))
            .setCacheConfiguration(pureCacheCfg, cityCfg, personCfg);
    }

    /** Test object unwrapped on interceptor side if applicable. */
    @Test
    public void testInterceptorUnwrapValIfNeeded() throws Exception {
        startGrid(0);
        IgniteEx client = startClientGrid("client");

        int incParam = 0;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            client.context().query().querySqlFields(new SqlFieldsQuery("insert into PUBLIC.PURE(id, name) values (?, 'val')")
                .setArgs(incParam++), keepBinary).getAll();
            client.context().query().querySqlFields(new SqlFieldsQuery("insert into PUBLIC.CITY(id, name) values (?, 'val')")
                .setArgs(incParam++), keepBinary).getAll();
            client.context().query().querySqlFields(new SqlFieldsQuery("insert into PUBLIC.PERSON(id, name, city_id) values (?, 'val', 1)")
                    .setArgs(incParam++), keepBinary).getAll();

            tx.commit();
        }

        client.context().query().querySqlFields(new SqlFieldsQuery("insert into PUBLIC.PURE(id, name) values (?, 'val')")
            .setArgs(incParam++), keepBinary).getAll();
        client.context().query().querySqlFields(new SqlFieldsQuery("insert into PUBLIC.CITY(id, name) values (?, 'val')")
            .setArgs(incParam++), keepBinary).getAll();
        client.context().query().querySqlFields(new SqlFieldsQuery("insert into PUBLIC.PERSON(id, name, city_id) values (?, 'val', 1)")
            .setArgs(incParam), keepBinary).getAll();
    }

    /** */
    private static class City {
        /** */
        @QuerySqlField
        String name;

        /** */
        City(String name) {
            this.name = name;
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField
        String name;

        /** */
        @QuerySqlField
        int city_id;

        /** */
        Person(String name, int city_id) {
            this.name = name;
            this.city_id = city_id;
        }
    }

    /** */
    private static class TestCacheInterceptor extends CacheInterceptorAdapter<Integer, Object> {
        /** */
        private final boolean keepBinary;

        /**
         * @param keepBinary Keep binary defines flag.
         */
        TestCacheInterceptor(boolean keepBinary) {
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Object onBeforePut(Cache.Entry<Integer, Object> entry, Object newVal) {
            assertEquals(keepBinary, newVal instanceof BinaryObject);

            return newVal;
        }
    }
}
