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

package org.apache.ignite.cache.query;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * {@link SqlQuery} unit tests.
 */
public class SqlQuerySelfTest {
    /** */
    private final String TYPE = "String";

    /** */
    private final Class TYPE_CL = String.class;

    /** */
    private final String QRY = "query";

    /** */
    private final String CACHE_NAME = "A1";

    /** */
    private final Class NULL_CL = null;

    /** */
    @Test(expected = NullPointerException.class)
    public void testFailedConstructionStringParams(){
        new SqlQuery<>(TYPE, null);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void testFailedConstructionClassParams(){
        new SqlQuery<>(TYPE_CL, null);
    }

    @Test(expected = NullPointerException.class)
    public void testFailedConstructionClassParams2() {
        new SqlQuery<>(NULL_CL, QRY);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void testFailedConstructionTreeParams(){
        new SqlQuery<>(CACHE_NAME, TYPE_CL, null);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void testFailedConstructionTreeParams2(){
        new SqlQuery<>(CACHE_NAME, null, QRY);
    }

    /** */
    @Test
    public void testConstructionStringParams(){
        final String nullStr = null;
        checkQuery( new SqlQuery<>("", QRY), null, "", QRY);
        checkQuery( new SqlQuery<>(TYPE, ""), null, TYPE, "");
        checkQuery( new SqlQuery<>(nullStr, QRY), null, null, QRY);
        checkQuery( new SqlQuery<>(TYPE, QRY), null, TYPE, QRY);
        checkQuery( new SqlQuery<>("."+ TYPE, QRY), "", TYPE, QRY);
        checkQuery( new SqlQuery<>(CACHE_NAME+"."+ TYPE, QRY), CACHE_NAME, TYPE, QRY);
        checkQuery( new SqlQuery<>(CACHE_NAME+".", QRY), CACHE_NAME, null, QRY);
    }

    /** */
    @Test
    public void testConstructionClassParams() {
        checkQuery( new SqlQuery<>(TYPE_CL, ""), null, TYPE, "");
        checkQuery( new SqlQuery<>(TYPE_CL, QRY), null, TYPE, QRY);
    }

    /** */
    @Test
    public void testConstructionTreeParams(){
        checkQuery( new SqlQuery<>(CACHE_NAME, TYPE_CL, ""), CACHE_NAME, TYPE, "");
        checkQuery( new SqlQuery<>(null, TYPE_CL, QRY), null, TYPE, QRY);
        checkQuery( new SqlQuery<>("", TYPE_CL, QRY), "", TYPE, QRY);
        checkQuery( new SqlQuery<>(CACHE_NAME, TYPE_CL, QRY), CACHE_NAME, TYPE, QRY);
    }

    /**
     *
     * @param q Created query.
     * @param cacheName Expected cache name.
     * @param type Expected type.
     * @param sql Expected sql.
     */
    private void checkQuery(SqlQuery<?,?> q, String cacheName, String type, String sql){
        assertEquals(cacheName, q.getCacheName());

        assertEquals(type, q.getType());

        assertEquals(sql, q.getSql());
    }
}
