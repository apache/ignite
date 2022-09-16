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

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.junit.Test;

/**
 * Planner test for SEARCH/SARG condition on indexed fields.
 */
public class SearchSargOnIndexTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema publicSchema;

    /** */
    private TestTable tbl;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
            .add("C1", f.createTypeWithNullability(f.createSqlType(SqlTypeName.INTEGER), true))
            .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
            .add("C3", f.createTypeWithNullability(f.createSqlType(SqlTypeName.INTEGER), true))
            .add("C4", f.createTypeWithNullability(f.createSqlType(SqlTypeName.INTEGER), true))
            .build();

        publicSchema = new IgniteSchema("PUBLIC");

        tbl = createTable(publicSchema, "TEST", type, IgniteDistributions.single(), null);

        tbl.addIndex("C1C2C3", 0, 1, 2);
    }

    /** Simple case on one field, without multi tuple SEARCH/SARG. */
    @Test
    public void testBoundsOneFieldSingleTuple() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 = 1", exact(1));

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C3 = 1", exact(1), empty(), empty(), empty());

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 <= 3",
            range(1, 3, false, true));

        assertBounds("SELECT * FROM TEST WHERE C1 < 3 AND C1 IS NOT NULL",
            range("null", 3, false, false));

        // Redundant "IS NOT NULL condition".
        assertBounds("SELECT * FROM TEST WHERE C1 > 3 AND C1 IS NOT NULL",
            range(3, null, false, true));

        // C4 field not in collation.
        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 <= 3 AND C4 = 1",
            range(1, 3, false, true),
            empty(),
            empty(),
            empty()
        );

        // Cannot proceed to C3 without C2.
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C3 = 1",
            exact(1),
            empty(),
            empty(),
            empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 <= 3 AND C3 = 1",
            range(1, 3, false, true),
            empty(),
            empty(),
            empty()
        );
    }

    /** Simple SEARCH/SARG. */
    @Test
    public void testBoundsOneFieldSearch() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3)",
            multi(exact(1), exact(2), exact(3)));

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C1 IS NOT NULL",
            multi(exact(1), exact(2), exact(3)));

        assertBounds("SELECT * FROM TEST WHERE (C1 > 1 AND C1 < 3) OR C1 IN (4, 5) OR C1 = 6 OR C1 > 7",
            multi(
                range(1, 3, false, false),
                exact(4),
                exact(5),
                exact(6),
                range(7, null, false, true)));
    }

    /** Simple SEARCH/SARG, values deduplication. */
    @Test
    public void testBoundsOneFieldSearchDeduplication() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3, 2, 1)",
            multi(exact(1), exact(2), exact(3)));
    }

    /** Simple SEARCH/SARG, range optimization. */
    @Test
    public void testBoundsOneFieldSearchRangeOptimization() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE (C1 > 1 AND C1 < 4) OR (C1 > 3 AND C1 < 5) OR (C1 > 7) OR (C1 > 6)",
            multi(
                range(1, 5, false, false),
                range(6, null, false, true)));

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 < 3 AND C1 <> 2",
            multi(
                range(1, 2, false, false),
                range(2, 3, false, false)));
    }

    /** Simple SEARCH/SARG with "IS NULL" condition. */
    @Test
    public void testBoundsOneFieldSearchWithNull() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) OR C1 IS NULL",
            multi(exact("null"), exact(1), exact(2), exact(3)),
            empty(),
            empty(),
            empty()
        );
    }

    /** SEARCH/SARG with DESC ordering. */
    @Test
    public void testBoundsDescOrdering() throws Exception {
        tbl.addIndex(RelCollations.of(TraitUtils.createFieldCollation(3, false),
            TraitUtils.createFieldCollation(2, true)), "C4");

        assertBounds("SELECT * FROM TEST WHERE C4 IN (1, 2, 3) AND C3 > 1",
            empty(),
            empty(),
            range(1, null, false, true),
            multi(exact(1), exact(2), exact(3))
        );

        assertBounds("SELECT * FROM TEST WHERE ((C4 > 1 AND C4 < 5) OR (C4 > 7 AND C4 < 9)) AND C3 = 1",
            empty(),
            empty(),
            empty(),
            multi(
                range(5, 1, false, false),
                range(9, 7, false, false))
        );
    }

    /** SEARCH/SARG with conditions on several fields. */
    @Test
    public void testBoundsSeveralFieldsSearch() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 IN ('a', 'b')",
            exact(1),
            multi(exact("a"), exact("b"))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 = 'a'",
            multi(exact(1), exact(2), exact(3)),
            exact("a")
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 IN ('a', 'b')",
            multi(exact(1), exact(2), exact(3)),
            multi(exact("a"), exact("b"))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 IN ('a', 'b') AND C3 IN (4, 5) AND C4 = 1",
            multi(exact(1), exact(2), exact(3)),
            multi(exact("a"), exact("b")),
            multi(exact(4), exact(5)),
            empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3) AND C2 IN ('a', 'b') AND C3 > 4",
            multi(exact(1), exact(2), exact(3)),
            multi(exact("a"), exact("b")),
            range(4, null, false, true)
        );

        // Cannot proceed to the next field after the range condition.
        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C2 = 'a'",
            range(1, null, false, true),
            empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C2 > 'a'",
            range(1, null, false, true),
            empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 = 'a'",
            range(1, null, true, true),
            empty() // TODO https://issues.apache.org/jira/browse/IGNITE-13568 Fix to exact("a")
        );

        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 > 'a'",
            range(1, null, true, true),
            empty() // TODO https://issues.apache.org/jira/browse/IGNITE-13568 Fix to range("a", null, false, true)
        );

        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 < 'a'",
            range(1, null, true, true),
            empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 >= 1 AND C2 IN ('a', 'b')",
            range(1, null, true, true),
            empty()
        );

        // Cannot proceed to the next field after SEARCH/SARG with range condition.
        assertBounds("SELECT * FROM TEST WHERE ((C1 > 1 AND C1 < 3) OR C1 > 5) AND C2 = 'a'",
            multi(
                range(1, 3, false, false),
                range(5, null, false, true)),
            empty()
        );
    }

    /** Test max complexity of SEARCH/SARG to include into index scan. */
    @Test
    public void testBoundsMaxComplexity() throws Exception {
        int limit = RexUtils.MAX_SEARCH_BOUNDS_COMPLEXITY;

        String inVals = String.join(", ", IntStream.range(0, limit + 1).mapToObj(Integer::toString)
            .toArray(String[]::new));

        assertPlan("SELECT * FROM TEST WHERE C1 IN (" + inVals + ")", publicSchema, isTableScan("TEST"));

        inVals = String.join(", ", IntStream.range(0, limit / 10 + 1).mapToObj(Integer::toString)
            .toArray(String[]::new));

        assertBounds("SELECT * FROM TEST WHERE C1 IN (1, 2, 3, 4, 5) AND C2 IN ('a', 'b') AND C3 IN (" + inVals + ")",
            multi(exact(1), exact(2), exact(3), exact(4), exact(5)),
            multi(exact("a"), exact("b")),
            empty()
        );
    }

    /** SEARCH/SARG with wrong literal types. */
    @Test
    public void testBoundsTypeConversion() throws Exception {
        assertBounds("SELECT * FROM TEST WHERE C1 IN ('1', 2, 3)",
            multi(exact(1), exact(2), exact(3))
        );

        // TODO
        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 > 1",
            exact(1),
            empty()
        );

        assertBounds("SELECT * FROM TEST WHERE C1 = 1 AND C2 IN (2, 3)",
            exact(1),
            empty()
        );
    }

    /** Test dynamic parameters search. */
    @Test
    public void testBoundsDynamicParams() throws Exception {
        // Cannot optimize dynamic parameters to SEARCH/SARG, query is splitted by or-to-union rule.
        assertPlan("SELECT * FROM TEST WHERE C1 IN (?, ?)", publicSchema, isInstanceOf(IgniteUnionAll.class)
            .and(input(0, isIndexScan("TEST", "C1C2C3")))
            .and(input(1, isIndexScan("TEST", "C1C2C3")))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 = ? AND C2 IN ('a', 'b')",
            exact("?0"),
            multi(exact("a"), exact("b"))
        );

        assertBounds("SELECT * FROM TEST WHERE C1 = ? AND C2 > ? AND C2 < ?",
            exact("?0"),
            range("?1", "?2", false, false)
        );
    }

    /** */
    private void assertBounds(String sql, Predicate<SearchBounds>... predicates) throws Exception {
        assertPlan(sql, publicSchema, isInstanceOf(IgniteIndexScan.class)
            .and(scan -> matchBounds(scan.searchBounds(), predicates)));
    }

    /** */
    private boolean matchBounds(List<SearchBounds> searchBounds, Predicate<SearchBounds>... predicates) {
        for (int i = 0; i < predicates.length; i++) {
            if (!predicates[i].test(searchBounds.get(i))) {
                lastErrorMsg = "Not expected bounds: " + searchBounds.get(i);

                return false;
            }
        }

        return true;
    }

    /** */
    private static Predicate<SearchBounds> empty() {
        return Objects::isNull;
    }

    /** */
    private static Predicate<SearchBounds> exact(Object val) {
        return b -> b instanceof ExactBounds && matchValue(val, ((ExactBounds)b).bound());
    }

    /** */
    private static Predicate<SearchBounds> range(Object lower, Object upper, boolean lowerInclude, boolean upperInclude) {
        return b -> b instanceof RangeBounds &&
            matchValue(lower, ((RangeBounds)b).lowerBound()) &&
            matchValue(upper, ((RangeBounds)b).upperBound()) &&
            lowerInclude == ((RangeBounds)b).lowerInclude() &&
            upperInclude == ((RangeBounds)b).upperInclude();
    }

    /** */
    private Predicate<SearchBounds> multi(Predicate<SearchBounds>... predicates) {
        return b -> b instanceof MultiBounds &&
            ((MultiBounds)b).bounds().size() == predicates.length &&
            matchBounds(((MultiBounds)b).bounds(), predicates);
    }

    /** */
    private static boolean matchValue(Object val, RexNode bound) {
        if (val == null || bound == null)
            return val == bound;

        bound = RexUtil.removeCast(bound);

        return Objects.toString(val).equals(Objects.toString(
            bound instanceof RexLiteral ? ((RexLiteral)bound).getValueAs(val.getClass()) : bound));
    }
}
