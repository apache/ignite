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
import java.util.Collections;
import org.apache.calcite.runtime.CalciteException;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.Test;

/** Tests multiset constructors, predicates, and functions, including duplicate multiplicities. */
public class MultisetIntegrationTest extends AbstractBasicIntegrationTest {
    /** An empty collection with a known element type. */
    private static final String EMPTY = "MULTISET(SELECT val FROM t WHERE FALSE)";

    /** A null collection with a known element type. */
    private static final String NULL_MULTISET = "CAST(NULL AS INTEGER MULTISET)";

    /** Collections with unequal multiplicities and null elements. */
    private static final String LEFT = "MULTISET[3, 1, 1, CAST(NULL AS INTEGER), CAST(NULL AS INTEGER)]";

    /** */
    private static final String RIGHT = "MULTISET[1, 2, CAST(NULL AS INTEGER)]";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE t(val INT)");
        sql("INSERT INTO t VALUES (1)");
    }

    /** Scalar constructors exercise the internal SLICE operation, which removes the temporary ROW wrapper. */
    @Test
    public void testConstructors() {
        assertExpression("MULTISET[val, val + 1, val]").returns(Arrays.asList(1, 2, 1)).check();
        assertExpression("CARDINALITY(MULTISET[val, CAST(NULL AS INTEGER)])").returns(2).check();
        assertQuery("SELECT x.v FROM t, UNNEST(MULTISET[val, val + 1, CAST(NULL AS INTEGER)]) AS x(v)")
            .returns(1).returns(2).returns(NULL_RESULT).check();
        assertQuery("SELECT * FROM UNNEST(MULTISET[ROW(1), ROW(2)])").returns(1).returns(2).check();
        assertQuery("SELECT * FROM UNNEST(MULTISET[ROW(1, 'a'), ROW(2, CAST(NULL AS VARCHAR))])")
            .returns(1, "a").returns(2, null).check();
        assertExpression(EMPTY).returns(Collections.emptyList()).check();
        assertExpression("CARDINALITY(" + EMPTY + ")").returns(0).check();
        assertExpression(EMPTY + " IS EMPTY").returns(true).check();
        assertExpression(EMPTY + " IS NOT EMPTY").returns(false).check();

        sql("INSERT INTO t VALUES (1), (2), (NULL)");

        assertMultiset("MULTISET(SELECT val FROM t)", 1, 1, 2, null);
        assertMultiset("MULTISET(SELECT DISTINCT val FROM t)", 1, 2, null);
        assertQuery("SELECT * FROM UNNEST(MULTISET(SELECT val, val + 10 FROM t))")
            .returns(1, 11).returns(1, 11).returns(2, 12).returns(null, null).check();
    }

    /** ELEMENT returns null for empty collections and rejects multiple elements, even equal ones. */
    @Test
    public void testElement() {
        assertExpression("ELEMENT(MULTISET[val + 10])").returns(11).check();
        assertExpression("ELEMENT(MULTISET[CAST(NULL AS INTEGER)])").returns(NULL_RESULT).check();
        assertExpression("ELEMENT(" + EMPTY + ")").returns(NULL_RESULT).check();
        assertExpression("ELEMENT(" + NULL_MULTISET + ")").returns(NULL_RESULT).check();

        assertThrows("SELECT ELEMENT(MULTISET[1, 2])", CalciteException.class, "More than one value");
        assertThrows("SELECT ELEMENT(MULTISET[1, 1])", CalciteException.class, "More than one value");
    }

    /** Field access on an extracted ROW exercises the internal STRUCT_ACCESS operation. */
    @Test
    public void testStructAccess() {
        String qry = "SELECT s.r.a, s.r.b FROM (SELECT ELEMENT(CAST(? AS " +
            "ROW(a INTEGER, b VARCHAR(10)) MULTISET)) AS r FROM t) AS s";

        assertQuery(qry).withParams(Collections.singletonList(new Object[] {1, "a"})).returns(1, "a").check();
        assertQuery(qry).withParams(Collections.singletonList(new Object[] {2, null})).returns(2, null).check();
        assertQuery(qry).withParams(Collections.emptyList()).returns(null, null).check();
        assertQuery(qry).withParams(new Object[] {null}).returns(null, null).check();
        assertQuery("SELECT s.r.a IS NULL FROM (SELECT ELEMENT(CAST(? AS " +
            "ROW(a INTEGER, b VARCHAR(10)) MULTISET)) AS r FROM t) AS s")
            .withParams(Collections.emptyList()).returns(true).check();
    }

    /** Membership and set predicates handle empty collections and distinguish one null from repeated nulls. */
    @Test
    public void testMembershipAndSetPredicates() {
        assertExpression("val MEMBER OF MULTISET[2, val, val]").returns(true).check();
        assertExpression("(val + 1) MEMBER OF MULTISET[val, val]").returns(false).check();
        assertExpression("val MEMBER OF " + EMPTY).returns(false).check();
        assertExpression("val MEMBER OF MULTISET[CAST(NULL AS INTEGER), val]").returns(true).check();
        assertExpression("val MEMBER OF " + NULL_MULTISET).returns(NULL_RESULT).check();
        assertExpression("CAST(NULL AS INTEGER) MEMBER OF MULTISET[val]").returns(NULL_RESULT).check();

        for (String multiset : Arrays.asList(EMPTY, "MULTISET[2, val]", "MULTISET[CAST(NULL AS INTEGER)]")) {
            assertExpression(multiset + " IS A SET").returns(true).check();
            assertExpression(multiset + " IS NOT A SET").returns(false).check();
        }

        for (String multiset : Arrays.asList("MULTISET[val, val]",
            "MULTISET[CAST(NULL AS INTEGER), CAST(NULL AS INTEGER)]")) {
            assertExpression(multiset + " IS A SET").returns(false).check();
            assertExpression(multiset + " IS NOT A SET").returns(true).check();
        }
    }

    /** UNION ALL adds multiplicities; DISTINCT removes repeats, including repeated nulls. */
    @Test
    public void testUnion() {
        for (String op : Arrays.asList("UNION", "UNION ALL")) {
            assertMultiset(LEFT + " MULTISET " + op + " " + RIGHT, 3, 1, 1, null, null, 1, 2, null);
            assertMultiset(LEFT + " MULTISET " + op + " " + EMPTY, 3, 1, 1, null, null);
            assertMultiset(EMPTY + " MULTISET " + op + " " + LEFT, 3, 1, 1, null, null);
        }

        assertMultiset(LEFT + " MULTISET UNION DISTINCT " + RIGHT, 3, 1, 2, null);
        assertMultiset(LEFT + " MULTISET UNION DISTINCT " + EMPTY, 3, 1, null);
        assertMultiset(EMPTY + " MULTISET UNION DISTINCT " + LEFT, 3, 1, null);
    }

    /** INTERSECT ALL takes the smaller multiplicity of each common element. */
    @Test
    public void testIntersect() {
        for (String op : Arrays.asList("INTERSECT", "INTERSECT ALL", "INTERSECT DISTINCT")) {
            assertMultiset(LEFT + " MULTISET " + op + " " + RIGHT, 1, null);
            assertMultiset(LEFT + " MULTISET " + op + " " + EMPTY);
            assertMultiset(EMPTY + " MULTISET " + op + " " + LEFT);
            assertMultiset("MULTISET[1, 1] MULTISET " + op + " MULTISET[2, 2]");
        }

        assertMultiset(LEFT + " MULTISET INTERSECT ALL " + LEFT, 3, 1, 1, null, null);
        assertMultiset(LEFT + " MULTISET INTERSECT DISTINCT " + LEFT, 3, 1, null);
    }

    /** EXCEPT ALL subtracts multiplicities; DISTINCT removes all matching values. */
    @Test
    public void testExcept() {
        for (String op : Arrays.asList("EXCEPT", "EXCEPT ALL")) {
            assertMultiset(LEFT + " MULTISET " + op + " " + RIGHT, 3, 1, null);
            assertMultiset(RIGHT + " MULTISET " + op + " " + LEFT, 2);
            assertMultiset(LEFT + " MULTISET " + op + " " + EMPTY, 3, 1, 1, null, null);
            assertMultiset(EMPTY + " MULTISET " + op + " " + LEFT);
            assertMultiset(LEFT + " MULTISET " + op + " " + LEFT);
        }

        assertMultiset(LEFT + " MULTISET EXCEPT DISTINCT " + RIGHT, 3);
        assertMultiset(LEFT + " MULTISET EXCEPT DISTINCT " + EMPTY, 3, 1, null);
        assertMultiset(EMPTY + " MULTISET EXCEPT DISTINCT " + LEFT);
    }

    /** SUBMULTISET checks multiplicities rather than only membership, independently of order. */
    @Test
    public void testSubmultiset() {
        for (String predicate : Arrays.asList("SUBMULTISET OF", "NOT SUBMULTISET OF")) {
            boolean negated = predicate.startsWith("NOT");

            assertExpression("MULTISET[2, 1, 1] " + predicate + " MULTISET[1, 2, 1]").returns(!negated).check();
            assertExpression("MULTISET[1, 1] " + predicate + " MULTISET[1, 2, 3]").returns(negated).check();
            assertExpression("MULTISET[4] " + predicate + " MULTISET[1, 2, 3]").returns(negated).check();
            assertExpression(EMPTY + " " + predicate + " MULTISET[val]").returns(!negated).check();
            assertExpression(EMPTY + " " + predicate + " " + EMPTY).returns(!negated).check();
            assertExpression("MULTISET[val] " + predicate + " " + EMPTY).returns(negated).check();
            assertExpression(RIGHT + " " + predicate + " " + LEFT).returns(negated).check();
            assertExpression("MULTISET[CAST(NULL AS INTEGER)] " + predicate + " " + LEFT)
                .returns(!negated).check();
            assertExpression(NULL_MULTISET + " " + predicate + " MULTISET[val]").returns(NULL_RESULT).check();
            assertExpression("MULTISET[val] " + predicate + " " + NULL_MULTISET).returns(NULL_RESULT).check();
        }
    }

    /** Null propagation is checked with computed operands so optimizers cannot rely on literal folding alone. */
    @Test
    public void testNullPropagation() {
        String nullable = "(CASE WHEN val = 1 THEN " + NULL_MULTISET + " ELSE MULTISET[val] END)";

        assertExpression("CARDINALITY(" + nullable + ")").returns(NULL_RESULT).check();

        for (String op : Arrays.asList("UNION ALL", "UNION DISTINCT", "INTERSECT ALL", "INTERSECT DISTINCT",
            "EXCEPT ALL", "EXCEPT DISTINCT")) {
            assertExpression(nullable + " MULTISET " + op + " MULTISET[val]").returns(NULL_RESULT).check();
            assertExpression("MULTISET[val] MULTISET " + op + " " + nullable).returns(NULL_RESULT).check();
        }

        for (String predicate : Arrays.asList("IS EMPTY", "IS NOT EMPTY", "IS A SET", "IS NOT A SET")) {
            assertExpression(NULL_MULTISET + " " + predicate).returns(NULL_RESULT).check();
            assertExpression(nullable + " " + predicate).returns(NULL_RESULT).check();
            assertExpression("(" + nullable + " " + predicate + ") IS NULL").returns(true).check();
        }

        // IS EMPTY shares its implementation with ARRAY, so preserve null propagation for that type too.
        assertExpression("CAST(NULL AS INTEGER ARRAY) IS EMPTY").returns(NULL_RESULT).check();
    }

    /** A typed collection parameter retains duplicates and supports empty and null collections. */
    @Test
    public void testParameters() {
        String qry = "SELECT * FROM UNNEST(CAST(? AS INTEGER MULTISET))";

        assertQuery(qry).withParams(Arrays.asList(2, 1, 1, null)).returns(2).returns(1).returns(1)
            .returns(NULL_RESULT).check();
        assertQuery(qry).withParams(Collections.emptyList()).resultSize(0).check();
        assertQuery(qry).withParams(new Object[] {null}).resultSize(0).check();
        assertExpression("MULTISET[?, ?, ?]").withParams(1, 1, 2).returns(Arrays.asList(1, 1, 2)).check();
        assertQuery("SELECT MULTISET[?, ?, ?]").withParams(1, 1, 2).returns(Arrays.asList(1, 1, 2)).check();
        assertQuery("SELECT CARDINALITY(MULTISET[?, ?, ?])").withParams(1, 1, 2).returns(3).check();
    }

    /** Tests expressions on a distributed table, including plan serialization. */
    private QueryChecker assertExpression(String expr) {
        return assertQuery("SELECT " + expr + " FROM t");
    }

    /** Compares the elements and their multiplicities without depending on the multiset's iteration order. */
    private void assertMultiset(String expr, Object... exp) {
        QueryChecker checker = assertQuery("SELECT * FROM UNNEST(" + expr + ")").resultSize(exp.length);

        for (Object val : exp)
            checker.returns(new Object[] {val});

        checker.check();
    }
}
