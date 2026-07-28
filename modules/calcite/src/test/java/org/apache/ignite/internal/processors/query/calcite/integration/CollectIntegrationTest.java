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

import org.junit.Test;

/**
 * Integration test for collect node.
 */
public class CollectIntegrationTest extends AbstractBasicIntegrationTest {
    /**
     * Tests that collect node correctly handles the case when downstream requests
     * limited number of rows, where collect must push one row and then
     * properly terminate downstream.
     */
    @Test
    public void testRequestLimitedRowsCountFromCollect() {
        sql("CREATE TABLE t(a INT)");

        sql("INSERT INTO t (a) VALUES (?)", 0);

        String sql = "SELECT /*+ CNL_JOIN */ ARRAY(SELECT a FROM t) FROM t LIMIT 1";

        assertQuery(sql).resultSize(1).check();

        /**
         * The data source size of 513 (buffer size + 1) is used to ensure that multiple batches are needed
         * on right hand of CNLJ to process all input rows, in this case left hand is not requested
         * immediately after endLeft() call.
         */
        for (int i = 1; i < 513; i++)
            sql("INSERT INTO t (a) VALUES (?)", i);

        assertQuery(sql).resultSize(1).check();
    }
}
