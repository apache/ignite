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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.Test;

/**
 * Checks that using {@link QueryUtils#KEY_FIELD_NAME} in condition will use
 * {@link QueryUtils#PRIMARY_KEY_INDEX pk index}.
 */
public class SearchByKeyFieldTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()
        );

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(sqlCfg);
    }

    /** */
    @Test
    public void testSimplePk() {
        sql("create table PUBLIC.PERSON(id int primary key, name varchar, age int)");

        for (int i = 0; i < 10; i++)
            sql("insert into PUBLIC.PERSON(id, name, age) values (?, ?, ?)", i, "foo" + i, 18 + i);

        List<List<?>> sqlRs = sql("select _key, id from PUBLIC.PERSON order by id");
        int _key = (Integer) sqlRs.get(7).get(0);
        int id = (Integer) sqlRs.get(7).get(1);

        assertEquals(7, _key);
        assertEquals(7, id);

        assertQuery("select id, name, age, _key from PUBLIC.PERSON where _key = ?")
            .withParams(_key)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", QueryUtils.PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", QueryUtils.KEY_FIELD_NAME)
            .returns(id, "foo7", 25, _key)
            .check();

        // Let's just make sure that PK search is not broken.
        assertQuery("select id, name, age, _key from PUBLIC.PERSON where id = ?")
            .withParams(id)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", QueryUtils.PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", QueryUtils.KEY_FIELD_NAME)
            .returns(id, "foo7", 25, _key)
            .check();
    }

    /** */
    @Test
    public void testCompositePk() {
        sql("create table PUBLIC.PERSON(id int, name varchar, age int, primary key(id, name))");

        for (int i = 0; i < 10; i++)
            sql("insert into PUBLIC.PERSON(id, name, age) values (?, ?, ?)", i, "foo" + i, 18 + i);

        List<List<?>> sqlRs = sql("select _key, id, name from PUBLIC.PERSON order by id");
        BinaryObject _key = (BinaryObject) sqlRs.get(6).get(0);
        int id = (Integer) sqlRs.get(6).get(1);
        String name = (String) sqlRs.get(6).get(2);

        assertEquals(6, id);
        assertEquals("foo6", name);

        assertQuery("select id, name, age, _key from PUBLIC.PERSON where _key = ?")
            .withParams(_key)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", QueryUtils.PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", QueryUtils.KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();

        // Let's just make sure that PK search is not broken.
        assertQuery("select id, name, age, _key from PUBLIC.PERSON where id = ? and name = ?")
            .withParams(id, name)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", QueryUtils.PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", QueryUtils.KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();
    }
}
