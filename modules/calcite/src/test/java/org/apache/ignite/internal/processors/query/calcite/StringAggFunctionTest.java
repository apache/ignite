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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_EXPERIMENTAL_SQL_ENGINE;

/** */
@WithSystemProperty(key = IGNITE_EXPERIMENTAL_SQL_ENGINE, value = "true")
public class StringAggFunctionTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        client = startClientGrid();
    }

    /** */
    @AfterClass
    public static void tearDown() {
        G.stopAll(false);
    }

    @Test
    public void test1() {
        execute(client, "CREATE TABLE names (name VARCHAR)");
        execute(client, "INSERT INTO names VALUES ('vlad'), ('tyaka'), ('test')");

//        System.out.println(execute(client, "select STRING_AGG('kek', ' , ');"));
        System.out.println(execute(client, "select STRING_AGG(name, ' , ') FROM names;"));

//        System.out.println(execute(client, "select COUNT(*) FROM names;"));

    }

    /** */
    @Test
    public void test2() throws InterruptedException {
        execute(client, "CREATE TABLE strings(g INTEGER, x VARCHAR, y VARCHAR);");
        execute(client, "INSERT INTO strings VALUES (1,'a','/'), (1,'b','-'), (2,'i','/'), (2,NULL,'-'), (2,'j','+'), (3,'p','/'), (4,'x','/'), (4,'y','-'), (4,'z','+')");

        System.out.println(execute(client, "SELECT STRING_AGG(x, y) FROM strings"));
    }

    /** */
    @Test
    public void test3() throws InterruptedException {
        execute(client, "CREATE TABLE strings(s VARCHAR);");
        execute(client, "INSERT INTO strings VALUES ('a'), ('b'), ('a');");

        System.out.println(execute(client, "SELECT STRING_AGG(DISTINCT s, ',') FROM strings"));
    }

    /** */
    @Test
    public void test4() throws InterruptedException {
        execute(client, "CREATE TABLE strings(s VARCHAR);");
        execute(client, "INSERT INTO strings VALUES ('a'), ('b'), ('c');");

        System.out.println(execute(client, "SELECT STRING_AGG(s, ',' ORDER BY s DESC) FROM strings"));
    }

    /** */
    @Test
    public void test5() throws InterruptedException {
        execute(client, "CREATE TABLE strings(s VARCHAR, z VARCHAR);");
        execute(client, "INSERT INTO strings VALUES ('a', 'a'), ('a', 'a'),  ('b', 'b'), ('c', 'c');");

        System.out.println(execute(client, "SELECT STRING_AGG(DISTINCT s, z ORDER BY z DESC) FROM strings"));
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
