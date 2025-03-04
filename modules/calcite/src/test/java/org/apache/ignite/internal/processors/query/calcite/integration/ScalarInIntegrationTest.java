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

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

/**
 * Test scalar IN operator.
 */
public class ScalarInIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE t(i INT, s VARCHAR)");
        sql("INSERT INTO t(i, s) VALUES (1, '1'), (3, '3'), (null, null)");
    }

    /** */
    @Test
    public void testInWithNull() {
        assertQuery("SELECT i FROM t WHERE i IN (0, 1, 2) OR i IS NULL")
            .returns(1).returns(NULL_RESULT).check();
        assertQuery("SELECT i FROM t WHERE i IN (0, 1, 2, NULL)")
            .returns(1).check();
        assertQuery("SELECT i FROM t WHERE i IN (0, 1, 2)")
            .returns(1).check();

        assertQuery("SELECT i, i IN (0, 1, 2) OR i IS NULL FROM t")
            .returns(1, true).returns(3, false).returns(null, true).check();
        assertQuery("SELECT i, i IN (0, 1, 2, NULL) FROM t")
            .returns(1, true).returns(3, null).returns(null, null).check();
        assertQuery("SELECT i, i IN (0, 1, 2) FROM t")
            .returns(1, true).returns(3, false).returns(null, null).check();

        assertQuery("SELECT s, s IN ('0', '1', '2') OR s IS NULL FROM t")
            .returns("1", true).returns("3", false).returns(null, true).check();
        assertQuery("SELECT s, s IN ('0', '1', '2', NULL) FROM t")
            .returns("1", true).returns("3", null).returns(null, null).check();
        assertQuery("SELECT s, s IN ('0', '1', '2') FROM t")
            .returns("1", true).returns("3", false).returns(null, null).check();

        assertQuery("SELECT i FROM t WHERE i NOT IN (0, 1, 2) OR i IS NULL")
            .returns(3).returns(NULL_RESULT).check();
        assertQuery("SELECT i FROM t WHERE i NOT IN (0, 1, 2, NULL)")
            .resultSize(0).check();
        assertQuery("SELECT i FROM t WHERE i NOT IN (0, 1, 2)")
            .returns(3).check();

        assertQuery("SELECT i, i NOT IN (0, 1, 2) OR i IS NULL FROM t")
            .returns(1, false).returns(3, true).returns(null, true).check();
        assertQuery("SELECT i, i NOT IN (0, 1, 2, NULL) FROM t")
            .returns(1, false).returns(3, null).returns(null, null).check();
        assertQuery("SELECT i, i NOT IN (0, 1, 2) FROM t")
            .returns(1, false).returns(3, true).returns(null, null).check();

        assertQuery("SELECT s, s NOT IN ('0', '1', '2') OR s IS NULL FROM t")
            .returns("1", false).returns("3", true).returns(null, true).check();
        assertQuery("SELECT s, s NOT IN ('0', '1', '2', NULL) FROM t")
            .returns("1", false).returns("3", null).returns(null, null).check();
        assertQuery("SELECT s, s NOT IN ('0', '1', '2') FROM t")
            .returns("1", false).returns("3", true).returns(null, null).check();
    }

    /** */
    @Test
    public void testLargeIn() {
        String in = IntStream.range(2, 1000).mapToObj(Integer::toString).collect(Collectors.joining(", "));

        assertQuery("SELECT i, i IN (" + in + ") FROM t")
            .returns(1, false).returns(3, true).returns(null, null).check();

        assertQuery("SELECT i, i NOT IN (" + in + ") FROM t")
            .returns(1, true).returns(3, false).returns(null, null).check();

        in = IntStream.range(2, 1000).mapToObj(i -> "'" + i + "'").collect(Collectors.joining(", "));

        assertQuery("SELECT s, s IN (" + in + ") FROM t")
            .returns("1", false).returns("3", true).returns(null, null).check();

        assertQuery("SELECT s, s NOT IN (" + in + ") FROM t")
            .returns("1", true).returns("3", false).returns(null, null).check();
    }

    /** */
    @Test
    public void testVariableDereference() {
        assertQuery("SELECT t1.s FROM t AS t1 LEFT JOIN t AS t2 ON t1.i=t2.i WHERE t2.s in ('1', '2')")
            .returns("1").check();
    }
}
