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

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** */
public class JoinIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** {@inheritDoc} */
    @Override protected void init() throws Exception {
        super.init();

        executeSql("create table t1 (c1 int, c2 int, c3 int) WITH " + atomicity());
        executeSql("create table t2 (c1 int, c2 int, c3 int) WITH " + atomicity());

        executeSql("create index t1_idx on t1 (c3, c2, c1)");
        executeSql("create index t2_idx on t2 (c3, c2, c1)");

        executeSql("insert into t1 values (1, 1, 1), (2, null, 2), (2, 2, 2), (3, 3, null), (3, 3, 3), (4, 4, 4)");
        executeSql("insert into t2 values (1, 1, 1), (2, 2, null), (2, 2, 2), (3, null, 3), (3, 3, 3), (4, 4, 4)");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        // NO-OP
    }

    /** */
    @ParameterizedTest
    @EnumSource(value = JoinType.class)
    public void testIsNotDistinctWithEquiConditionFrom(JoinType joinType) {
        // 'IS NOT DISTINCT' is first.
        assertQuery("select t1.c2, t1.c3, t2.c2, t2.c3 from t1 join t2 on t1.c2 is not distinct from t2.c2 and t1.c3 = t2.c3 - 1", joinType)
            .returns(null, 2, null, 3)
            .check();

        // 'IS NOT DISTINCT' is second.
        assertQuery("select t1.c2, t1.c3, t2.c2, t2.c3 from t1 join t2 on t1.c3 = t2.c3 - 1 and t1.c2 is not distinct from t2.c2", joinType)
            .returns(null, 2, null, 3)
            .check();

        // Duplicated condition.
        assertQuery("select t1.c2, t1.c3, t2.c2, t2.c3 from t1 join t2 on t1.c2 is not distinct from t2.c2 " +
            "and t1.c2 is not distinct from t2.c2 and t1.c3 = t2.c3 - 1", joinType)
            .returns(null, 2, null, 3)
            .check();
        assertQuery("select t1.c2, t1.c3, t2.c2, t2.c3 from t1 join t2 on t1.c3 = t2.c3 - 1 and t1.c3 = t2.c3 - 1" +
            "and t1.c2 is not distinct from t2.c2 and t1.c2 is not distinct from t2.c2", joinType)
            .returns(null, 2, null, 3)
            .check();

        // Other fields.
        assertQuery("select t1.c2, t1.c3, t2.c1, t2.c2 from t1 join t2 on t1.c3 is not distinct from t2.c2 - 1 " +
            "and t1.c2 = t2.c1", joinType)
            .returns(3, null, 3, null)
            .check();
        assertQuery("select t1.c2, t1.c3, t2.c2, t2.c3 from t1 join t2 " +
            "on t1.c2 is not distinct from t2.c2 and t1.c2 = t2.c2 and t1.c3 = t2.c3 and t1.c3 is not distinct from t2.c3",
                joinType)
            .returns(1, 1, 1, 1)
            .returns(2, 2, 2, 2)
            .returns(3, 3, 3, 3)
            .returns(4, 4, 4, 4)
            .check();

        // Two 'IS NOT DISTINCT's.
        assertQuery("select t1.c2, t1.c3, t2.c2, t2.c3 from t1 join t2 on t1.c3 is not distinct from t2.c3 - 1 " +
            "and t1.c2 is not distinct from t2.c2", joinType)
            .returns(null, 2, null, 3)
            .check();
    }

    /**
     * Test verifies result of inner join with different ordering.
     */
    @ParameterizedTest
    @EnumSource(value = JoinType.class)
    public void testInnerJoin(JoinType joinType) {
        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1, t1.c2, t1.c3", joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1, t1.c2, t1.c3 nulls first", joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1, t1.c2, t1.c3 nulls last", joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, null, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1 desc, t1.c2, t1.c3", joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1 desc, t1.c2, t1.c3 nulls first", joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1 desc, t1.c2, t1.c3 nulls last", joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, null, 3, 3)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t2.c3 c23, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3, t1.c2", joinType
        )
            .ordered()
            .returns(1, 1, 1, 1)
            .returns(2, 2, 2, 2)
            .returns(3, 3, 3, 3)
            .returns(4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
            joinType
        )
            .ordered()
            .returns(null, 3, 3, 3, 3)
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(null, 3, 3, 3, 3)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  join t2 " +
            "    on t1.c2 = t2.c2 " +
            " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(null, 3, 3, 3, 3)
            .check();
    }

    /**
     * Test verifies result of left join with different ordering.
     */
    @ParameterizedTest
    @EnumSource
    public void testLeftJoin(JoinType joinType) {
        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1, t1.c2, t1.c3",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, null, 2, null, null)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1, t1.c2 nulls first, t1.c3 nulls first",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, null, 2, null, null)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1, t1.c2 nulls last, t1.c3 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(2, null, 2, null, null)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, null, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1 desc, t1.c2, t1.c3",
            joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(2, null, 2, null, null)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1 desc, t1.c2, t1.c3 nulls first",
            joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(3, 3, null, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(2, null, 2, null, null)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c1 desc, t1.c2, t1.c3 nulls last",
            joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, null, 3, 3)
            .returns(2, null, 2, null, null)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t2.c3 c23, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3, t1.c2",
            joinType
        )
            .ordered()
            .returns(null, 3, null, null)
            .returns(1, 1, 1, 1)
            .returns(2, null, null, null)
            .returns(2, 2, 2, 2)
            .returns(3, 3, 3, 3)
            .returns(4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
            joinType
        )
            .ordered()
            .returns(null, 3, 3, null, null)
            .returns(1, 1, 1, 1, 1)
            .returns(2, null, 2, null, null)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, null, 2, null, null)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(null, 3, 3, null, null)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3 nulls first, t1.c2 nulls first, t1.c1 nulls first",
            joinType
        )
            .ordered()
            .returns(null, 3, 3, 3, 3)
            .returns(1, 1, 1, 1, 1)
            .returns(2, null, 2, null, null)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(2, null, 2, null, null)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(null, 3, 3, 3, 3)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22 " +
            "  from t1 " +
            "  left join t2 " +
            "    on t1.c2 = t2.c2 " +
            " order by t1.c3 nulls last, t1.c2 nulls last, t1.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2)
            .returns(2, null, 2, null, null)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(null, 3, 3, 3, 3)
            .check();
    }

    /**
     * Test verifies result of right join with different ordering.
     */
    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = "CORRELATED")
    public void testRightJoin(JoinType joinType) {
        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c1, t2.c2, t2.c3", joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, null)
            .returns(2, 2, 2, 2, 2)
            .returns(null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c1, t2.c2 nulls first, t2.c3 nulls first", joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, null)
            .returns(2, 2, 2, 2, 2)
            .returns(null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c1, t2.c2 nulls last, t2.c3 nulls last", joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, null)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(null, null, 3, null, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c1 desc, t2.c2, t2.c3", joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(2, 2, 2, 2, null)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c1 desc, t2.c2, t2.c3 nulls first", joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(2, 2, 2, 2, null)
            .returns(2, 2, 2, 2, 2)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c1 desc, t2.c2, t2.c3 nulls last", joinType
        )
            .ordered()
            .returns(4, 4, 4, 4, 4)
            .returns(null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, null)
            .returns(1, 1, 1, 1, 1)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t2.c3 c23, t2.c2 c22 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c3, t2.c2", joinType
        )
            .ordered()
            .returns(null, null, null, 2)
            .returns(1, 1, 1, 1)
            .returns(2, 2, 2, 2)
            .returns(null, null, 3, null)
            .returns(3, 3, 3, 3)
            .returns(4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c3 nulls first, t2.c2 nulls first, t2.c1 nulls first",
            joinType
        )
            .ordered()
            .returns(null, null, 2, 2, null)
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c3 = t2.c3 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c3 nulls last, t2.c2 nulls last, t2.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(null, null, 3, null, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(null, null, 2, 2, null)
            .check();

        assertQuery("" +
            "select t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c3 nulls first, t2.c2 nulls first, t2.c1 nulls first",
            joinType
        )
            .ordered()
            .returns(2, 2, 2, 2, null)
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            " order by t2.c3 nulls last, t2.c2 nulls last, t2.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(null, null, 3, null, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(2, 2, 2, 2, null)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            "   and t1.c3 = t2.c3 " +
            " order by t2.c3 nulls first, t2.c2 nulls first, t2.c1 nulls first",
            joinType
        )
            .ordered()
            .returns(null, null, null, 2, 2, null)
            .returns(1, 1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2, 2)
            .returns(null, null, null, 3, null, 3)
            .returns(3, 3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4, 4)
            .check();

        assertQuery("" +
            "select t1.c3 c13, t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c1 = t2.c1 " +
            "   and t1.c2 = t2.c2 " +
            "   and t1.c3 = t2.c3 " +
            " order by t2.c3 nulls last, t2.c2 nulls last, t2.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3, 3)
            .returns(null, null, null, 3, null, 3)
            .returns(4, 4, 4, 4, 4, 4)
            .returns(null, null, null, 2, 2, null)
            .check();

        assertQuery("" +
            "select t1.c2 c12, t1.c1 c11, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " right join t2 " +
            "    on t1.c2 = t2.c2 " +
            " order by t2.c3 nulls last, t2.c2 nulls last, t2.c1 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3)
            .returns(3, 3, 3, 3, 3)
            .returns(null, null, 3, null, 3)
            .returns(4, 4, 4, 4, 4)
            .returns(2, 2, 2, 2, null)
            .check();
    }

    /**
     * Test verifies result of full join.
     */
    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = "CORRELATED")
    public void testFullJoin(JoinType joinType) {
        assertQuery("" +
            "select t1.c1 c11, t1.c2 c12, t1.c3 c13, t2.c1 c21, t2.c2 c22, t2.c3 c23 " +
            "  from t1 " +
            " full join t2 " +
            "    on t1.c2 = t2.c2 " +
            " order by " +
            "    t1.c1 nulls last, t1.c2 nulls last, t1.c3 nulls last, t2.c1 nulls last, t2.c2 nulls last, t2.c3 nulls last",
            joinType
        )
            .ordered()
            .returns(1, 1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2, 2)
            .returns(2, 2, 2, 2, 2, null)
            .returns(2, null, 2, null, null, null)
            .returns(3, 3, 3, 3, 3, 3)
            .returns(3, 3, null, 3, 3, 3)
            .returns(4, 4, 4, 4, 4, 4)
            .returns(null, null, null, 3, null, 3)
            .check();
    }

    /**
     * Tests JOIN with USING clause.
     */
    @ParameterizedTest
    @EnumSource
    public void testJoinWithUsing(JoinType joinType) {
        // Select all join columns.
        assertQuery("SELECT * FROM t1 JOIN t2 USING (c1, c2)", joinType)
            .returns(1, 1, 1, 1)
            .returns(2, 2, 2, null)
            .returns(2, 2, 2, 2)
            .returns(3, 3, null, 3)
            .returns(3, 3, 3, 3)
            .returns(4, 4, 4, 4)
            .check();

        // Select all table columns explicitly.
        assertQuery("SELECT t1.*, t2.* FROM t1 JOIN t2 USING (c1, c2)", joinType)
            .returns(1, 1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2, null)
            .returns(2, 2, 2, 2, 2, 2)
            .returns(3, 3, null, 3, 3, 3)
            .returns(3, 3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4, 4)
            .check();

        // Select explicit columns. Columns from using - not ambiguous.
        assertQuery("SELECT c1, c2, t1.c3, t2.c3 FROM t1 JOIN t2 USING (c1, c2) ORDER BY c1, c2", joinType)
            .returns(1, 1, 1, 1)
            .returns(2, 2, 2, null)
            .returns(2, 2, 2, 2)
            .returns(3, 3, null, 3)
            .returns(3, 3, 3, 3)
            .returns(4, 4, 4, 4)
            .check();
    }

    /**
     * Tests NATURAL JOIN.
     */
    @ParameterizedTest
    @EnumSource
    public void testNatural(JoinType joinType) {
        // Select all join columns.
        assertQuery("SELECT * FROM t1 NATURAL JOIN t2", joinType)
            .returns(1, 1, 1)
            .returns(2, 2, 2)
            .returns(3, 3, 3)
            .returns(4, 4, 4)
            .check();

        // Select all tables columns explicitly.
        assertQuery("SELECT t1.*, t2.* FROM t1 NATURAL JOIN t2", joinType)
            .returns(1, 1, 1, 1, 1, 1)
            .returns(2, 2, 2, 2, 2, 2)
            .returns(3, 3, 3, 3, 3, 3)
            .returns(4, 4, 4, 4, 4, 4)
            .check();

        // Select explicit columns.
        assertQuery("SELECT t1.c1, t2.c2, t1.c3, t2.c3 FROM t1 NATURAL JOIN t2", joinType)
            .returns(1, 1, 1, 1)
            .returns(2, 2, 2, 2)
            .returns(3, 3, 3, 3)
            .returns(4, 4, 4, 4)
            .check();

        // Columns - not ambiguous.
        // TODO https://issues.apache.org/jira/browse/CALCITE-4915
        //assertQuery("SELECT c1, c2, c3 FROM t1 NATURAL JOIN t2 ORDER BY c1, c2, c3")
        //    .returns(1, 1, 1)
        //    .returns(2, 2, 2)
        //    .returns(3, 3, 3)
        //    .returns(4, 4, 4)
        //    .check();
    }

    /** */
    private QueryChecker assertQuery(String qry, JoinType joinType) {
        return assertQuery(qry.replace("select", "select "
            + Arrays.stream(joinType.disabledRules).collect(Collectors.joining("','", "/*+ DISABLE_RULE('", "') */"))));
    }

    /** */
    public enum JoinType {
        /** */
        NESTED_LOOP(
            "CorrelatedNestedLoopJoin",
            "JoinCommuteRule",
            "MergeJoinConverter",
            "HashJoinConverter"
        ),

        /** */
        MERGE(
            "CorrelatedNestedLoopJoin",
            "JoinCommuteRule",
            "NestedLoopJoinConverter",
            "HashJoinConverter"
        ),

        /** */
        CORRELATED(
            "MergeJoinConverter",
            "JoinCommuteRule",
            "NestedLoopJoinConverter",
            "HashJoinConverter"
        ),

        /** */
        HASH(
            "MergeJoinConverter",
            "JoinCommuteRule",
            "NestedLoopJoinConverter",
            "CorrelatedNestedLoopJoin"
        );

        /** */
        private final String[] disabledRules;

        /** */
        JoinType(String... disabledRules) {
            this.disabledRules = disabledRules;
        }
    }
}
