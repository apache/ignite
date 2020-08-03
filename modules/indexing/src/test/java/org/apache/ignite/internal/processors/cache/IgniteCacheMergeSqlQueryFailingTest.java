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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheMergeSqlQueryFailingTest extends IgniteCacheAbstractInsertSqlQuerySelfTest {
    /**
     *
     */
    @Test
    public void testSqlMergeIntoQuery() throws Exception {
        ignite(0).cache("S2P").query(new SqlFieldsQuery("CREATE TABLE " +
                "USERPUBSTATICDATA (BOOK VARCHAR, DESK VARCHAR, TRADERS VARCHAR, REGION VARCHAR, LOB VARCHAR, " +
                "EXCLUDE VARCHAR, TRANSIT VARCHAR, MAPBOOKTOTHISBOOK VARCHAR, " +
                "CONSTRAINT USERPUBSTATICDATA_PK PRIMARY KEY (BOOK,DESK)) " +
                "WITH \"template=replicated\"").setSchema("PUBLIC"));

        Ignite ignite = startClientGrid(3);

        IgniteCache srvCache = ignite(0).cache("S2P").withKeepBinary();

        IgniteCache clientCache = ignite.cache("S2P").withKeepBinary();

        srvCache.query(new SqlFieldsQuery(
            "MERGE INTO USERPUBSTATICDATA(BOOK, DESK, TRADERS, REGION, LOB, EXCLUDE, TRANSIT, MAPBOOKTOTHISBOOK) " +
                "VALUES('CADOIS', 'FRT TOR', 'Robin Das/Dave Carlson', 'Toronto', 'FRT', null, null, 'CADOIS');").setSchema("PUBLIC"));

        srvCache.query(new SqlFieldsQuery(
            "MERGE INTO USERPUBSTATICDATA(BOOK, DESK, TRADERS, REGION, LOB, EXCLUDE, TRANSIT, MAPBOOKTOTHISBOOK) " +
                "VALUES('CADOIS', 'FRT TOR 1', 'Robin Das/Dave Carlson 1', 'Toronto', 'FRT', null, null, 'CADOIS');").setSchema("PUBLIC"));

        clientCache.query(new SqlFieldsQuery(
            "MERGE INTO USERPUBSTATICDATA(BOOK, DESK, TRADERS, REGION, LOB, EXCLUDE, TRANSIT, MAPBOOKTOTHISBOOK) " +
                "VALUES('CADOIS', 'FRT TOR', 'Robin Das/Dave Carlson 2', 'Toronto', 'FRT', null, null, 'CADOIS');").setSchema("PUBLIC"));

        clientCache.query(new SqlFieldsQuery(
            "MERGE INTO USERPUBSTATICDATA(BOOK, TRADERS, REGION, LOB, EXCLUDE, TRANSIT, MAPBOOKTOTHISBOOK) " +
                "VALUES('CADOIS', 'Robin Das/Dave Carlson 2', 'Toronto', 'FRT', null, null, 'CADOIS');").setSchema("PUBLIC"));

        List<List<?>> res = clientCache.query(
            new SqlFieldsQuery("SELECT TRADERS FROM USERPUBSTATICDATA WHERE BOOK='CADOIS'").setSchema("PUBLIC")).getAll();

        assertEquals("Robin Das/Dave Carlson 2", res.get(0).get(0));
    }
}
