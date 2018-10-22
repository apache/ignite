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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check that affinity key works properly with cache templates.
 */
public class IgniteTemplateWithAffinityTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(LOCAL_IP_FINDER))
            .setCacheConfiguration(new CacheConfiguration("myCacheTemplate*"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    public void testPutBinaryThenInsertPartitioned() throws Exception {
        checkPutBinaryThenInsert("partitioned");
    }

    /** */
    public void testPutBinaryThenInsertReplicated() throws Exception {
        checkPutBinaryThenInsert("replicated");
    }

    /** */
    public void testPutBinaryThenInsertCustom() throws Exception {
        checkPutBinaryThenInsert("myCacheTemplate");
    }

    /** */
    private void checkPutBinaryThenInsert(String templateName) throws Exception {
        startGrid();

        executeSql("CREATE TABLE IF NOT EXISTS PUBLIC.mytable ("
            + "  \"aff_key\" VARCHAR(24) NOT NULL,"
            + "  \"key\" VARCHAR(24) NOT NULL,"
            + "  \"data\" VARCHAR(100),"
            + "  PRIMARY KEY(\"aff_key\", \"key\"))"
            + "WITH \"template=" + templateName + ", affinity_key=aff_key, cache_name=sqlcache, key_type=mykey, value_type=myvalue\";");

        grid().cache("sqlcache").withKeepBinary().put(
            grid().binary().builder("mykey")
                .setField("aff_key", "1")
                .setField("key", "2")
                .build(),
            grid().binary().builder("myvalue")
                .setField("data", "abc")
                .build()
        );

        executeSql("insert into PUBLIC.mytable (\"aff_key\", \"key\", \"data\") values ('1', '3', 'xyz');");

        assertEquals(2, executeSqlSingleValue("SELECT COUNT(*) FROM PUBLIC.mytable;"));

        assertEquals(2, executeSqlSingleValue("SELECT COUNT(*) FROM PUBLIC.mytable WHERE \"aff_key\" = '1';"));
    }

    /** */
    public void testInsertThenPutBinaryPartitioned() throws Exception {
        checkInsertThenPutBinary("partitioned");
    }

    /** */
    public void testInsertThenPutBinaryReplicated() throws Exception {
        checkInsertThenPutBinary("replicated");
    }

    /** */
    public void testInsertThenPutBinaryCustom() throws Exception {
        checkInsertThenPutBinary("myCacheTemplate");
    }

    /** */
    private void checkInsertThenPutBinary(String templateName) throws Exception {
        startGrid();

        executeSql("CREATE TABLE IF NOT EXISTS PUBLIC.mytable ("
            + "  \"aff_key\" VARCHAR(24) NOT NULL,"
            + "  \"key\" VARCHAR(24) NOT NULL,"
            + "  \"data\" VARCHAR(100),"
            + "  PRIMARY KEY(\"aff_key\", \"key\"))"
            + "WITH \"template=" + templateName + ", affinity_key=aff_key, cache_name=sqlcache, key_type=mykey, value_type=myvalue\";");

        executeSql("insert into PUBLIC.mytable (\"aff_key\", \"key\", \"data\") values ('1', '3', 'xyz');");

        grid().cache("sqlcache").withKeepBinary().put(
            grid().binary().builder("mykey")
                .setField("aff_key", "1")
                .setField("key", "2")
                .build(),
            grid().binary().builder("myvalue")
                .setField("data", "abc")
                .build()
        );

        assertEquals(2, executeSqlSingleValue("SELECT COUNT(*) FROM PUBLIC.mytable;"));

        assertEquals(2, executeSqlSingleValue("SELECT COUNT(*) FROM PUBLIC.mytable WHERE \"aff_key\" = '1';"));
    }

    /** */
    private void executeSql(String sql) {
        System.out.println(sql);

        List<List<?>> res = grid().getOrCreateCache("proxyCache").query(new SqlFieldsQuery(sql)).getAll();

        System.out.println(res);
    }

    /** */
    private long executeSqlSingleValue(String sql) {
        System.out.println(sql);

        List<List<?>> res = grid().getOrCreateCache("proxyCache").query(new SqlFieldsQuery(sql)).getAll();

        System.out.println(res);

        return (Long)res.get(0).get(0);
    }
}
