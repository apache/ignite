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

import java.util.Collections;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Integration test for UNNEST operator.
 */
public class UnnestIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testUnnestSingleCollection() {
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3])").returns(1).returns(2).returns(3).check();
        assertQuery("SELECT * FROM UNNEST(MAP['a', 1, 'b', 2])").returns("a", 1).returns("b", 2).check();
        assertQuery("SELECT * FROM UNNEST(ARRAY[ROW(1, 2), ROW(3, 4)])").returns(1, 2).returns(3, 4).check();

        // Dynamic parameters.
        assertQuery("SELECT * FROM UNNEST(?)").withParams(F.asList(1, 2)).returns(1).returns(2).check();
        assertQuery("SELECT * FROM UNNEST(?)").withParams(F.asMap("a", 1, "b", 2))
            .returns("a", 1).returns("b", 2).check();
        // Can't check dynamic parameters with ROW, since generic type of List can't be obtained in runtime.
        // SQL type of parameter F.asList(new Object[] {1, 2}) will be derived as array of scalars
        // (instead of array of rows) and UNNEST can only produce rows based on type derived on planner phase.

        // Subquery.
        assertQuery("SELECT * FROM UNNEST(SELECT ARRAY_AGG(a) FROM (VALUES (1), (2)) t(a))")
            .returns(1).returns(2).check();

        assertQuery("SELECT * FROM UNNEST(SELECT ARRAY_AGG(ROW(t.a, t.b)) FROM (VALUES (1, 2), (3, 4)) t(a, b))")
            .returns(1, 2).returns(3, 4).check();

        assertQuery("SELECT * FROM UNNEST(SELECT * FROM (VALUES (ARRAY[1, 2, 3]), (ARRAY[4, 5])))")
            .returns(1).returns(2).returns(3).returns(4).returns(5).check();

        assertQuery("SELECT * FROM UNNEST(SELECT * FROM (VALUES (MAP[1, 2, 3, 4]), (MAP[5, 6])))")
            .returns(1, 2).returns(3, 4).returns(5, 6).check();

        assertQuery("SELECT * FROM UNNEST(SELECT * FROM (VALUES (ARRAY[ROW(1, 2), ROW(3, 4)]), (ARRAY[ROW(5, 6)])))")
            .returns(1, 2).returns(3, 4).returns(5, 6).check();
    }

    /** */
    @Test
    public void testUnnestMultiCollection() {
        assertQuery("SELECT * FROM UNNEST(ARRAY[1], ARRAY[2])").returns(1, 2).check();
        assertQuery("SELECT * FROM UNNEST(ARRAY[1], ARRAY[2, 3])").returns(1, 2).returns(1, 3).check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5])")
            .returns(1, 4).returns(1, 5)
            .returns(2, 4).returns(2, 5)
            .returns(3, 4).returns(3, 5)
            .check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3], ?)").withParams(F.asList(4, 5))
            .returns(1, 4).returns(1, 5)
            .returns(2, 4).returns(2, 5)
            .returns(3, 4).returns(3, 5)
            .check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3], MAP[4, 5, 6, 7])")
            .returns(1, 4, 5).returns(1, 6, 7)
            .returns(2, 4, 5).returns(2, 6, 7)
            .returns(3, 4, 5).returns(3, 6, 7)
            .check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[ROW(1, 2), ROW(3, 4)], MAP[5, 6, 7, 8], ARRAY[9, 10])")
            .returns(1, 2, 5, 6, 9).returns(1, 2, 5, 6, 10)
            .returns(1, 2, 7, 8, 9).returns(1, 2, 7, 8, 10)
            .returns(3, 4, 5, 6, 9).returns(3, 4, 5, 6, 10)
            .returns(3, 4, 7, 8, 9).returns(3, 4, 7, 8, 10)
            .check();
    }

    /** */
    @Test
    public void testUnnestMultiLineMultiCollection() {
        assertQuery("SELECT c, d FROM (VALUES (ARRAY[1, 2], ARRAY[3, 4]), (ARRAY[5, 6], ARRAY[7])) v(a, b), " +
            "UNNEST(v.a, v.b) u(c, d)")
            .returns(1, 3).returns(1, 4).returns(2, 3).returns(2, 4)
            .returns(5, 7).returns(6, 7)
            .check();

        assertQuery("SELECT c, d, e FROM (VALUES (MAP[1, 2, 3, 4], ARRAY[5, 6]), (MAP[7, 8, 9, 10], ARRAY[11])) v(a, b), " +
            "UNNEST(v.a, v.b) u(c, d, e)")
            .returns(1, 2, 5).returns(1, 2, 6).returns(3, 4, 5).returns(3, 4, 6)
            .returns(7, 8, 11).returns(9, 10, 11)
            .check();
    }

    /** */
    @Test
    public void testUnnestEmptyCollection() {
        assertQuery("SELECT * FROM UNNEST(SELECT ARRAY_AGG(a) FROM (VALUES (1)) t(a) WHERE a = 0)")
            .resultSize(0).check();

        assertQuery("SELECT * FROM (SELECT ARRAY_AGG(a) a FROM (VALUES (1)) t(a) WHERE a = 0) T, " +
            "UNNEST(T.a, ARRAY[1, 2, 3])")
            .resultSize(0).check();

        assertQuery("SELECT * FROM (SELECT ARRAY_AGG(a) a FROM (VALUES (1)) t(a) WHERE a = 0) T, " +
            "UNNEST(T.a, MAP[1, 2])")
            .resultSize(0).check();

        assertQuery("SELECT * FROM UNNEST(?)")
            .withParams(Collections.emptyList()).resultSize(0).check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3], ?)")
            .withParams(Collections.emptyList()).resultSize(0).check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3], ?)")
            .withParams(Collections.emptyMap()).resultSize(0).check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[ROW(1, 2), ROW(3, 4)], MAP[1, 2], ?)")
            .withParams(Collections.emptyList()).resultSize(0).check();
    }

    /** */
    @Test
    public void testUnnestWithOrdinality() {
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) WITH ORDINALITY")
            .returns(1, 4, 1).returns(1, 5, 2)
            .returns(2, 4, 3).returns(2, 5, 4)
            .returns(3, 4, 5).returns(3, 5, 6)
            .check();

        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3], MAP[4, 5, 6, 7]) WITH ORDINALITY")
            .returns(1, 4, 5, 1).returns(1, 6, 7, 2)
            .returns(2, 4, 5, 3).returns(2, 6, 7, 4)
            .returns(3, 4, 5, 5).returns(3, 6, 7, 6)
            .check();

        assertQuery("SELECT c, d, e FROM (VALUES (ARRAY[1, 2], ARRAY[3, 4]), (ARRAY[5, 6], ARRAY[7])) v(a, b), " +
            "UNNEST(v.a, v.b) WITH ORDINALITY u(c, d, e)")
            .returns(1, 3, 1).returns(1, 4, 2).returns(2, 3, 3).returns(2, 4, 4)
            .returns(5, 7, 1).returns(6, 7, 2)
            .check();
    }

    /** */
    @Test
    public void testTableJoin() {
        sql("CREATE TABLE t(id INT, val VARCHAR, PRIMARY KEY(id))");

        for (int i = 0; i < 1000; i++)
            sql("INSERT INTO t VALUES (?, ?)", i, "val" + i);

        assertQuery("SELECT * FROM t WHERE id IN (SELECT * FROM UNNEST(ARRAY[10, 20, 30]))")
            .returns(10, "val10").returns(20, "val20").returns(30, "val30")
            .check();

        assertQuery("SELECT * FROM t WHERE id IN (SELECT * FROM UNNEST(?))")
            .withParams(F.asList(10, 20, 30))
            .returns(10, "val10").returns(20, "val20").returns(30, "val30")
            .check();

        assertQuery("SELECT t.* FROM UNNEST(?) u(a) JOIN t ON t.id = u.a")
            .withParams(F.asList(10, 20, 30))
            .returns(10, "val10").returns(20, "val20").returns(30, "val30")
            .check();
    }
}
