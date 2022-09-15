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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
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

        assertBounds("SELECT * FROM TEST WHERE C1 > 3 AND C1 IS NOT NULL",
            range(3, null, false, true));

        assertBounds("SELECT * FROM TEST WHERE C1 > 1 AND C1 <= 3 AND C4 = 1",
            range(1, 3, false, true),
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

    /** */
    private void assertBounds(String sql, Predicate<SearchBounds>... predicates) throws Exception {
        assertPlan(sql, publicSchema, isInstanceOf(IgniteIndexScan.class)
            .and(scan -> matchBounds(scan.searchBounds(), predicates)));
    }

    /** */
    private static boolean matchBounds(List<SearchBounds> searchBounds, Predicate<SearchBounds>... predicates) {
        for (int i = 0; i < predicates.length; i++) {
            if (!predicates[i].test(searchBounds.get(i)))
                return false;
        }

        return true;
    }

    /** */
    private static Predicate<SearchBounds> empty() {
        return Objects::isNull;
    }

    /** */
    private static Predicate<SearchBounds> exact(Object val) {
        return b -> b instanceof ExactBounds && matchLiteral(val, ((ExactBounds)b).bound());
    }

    /** */
    private static Predicate<SearchBounds> range(Object lower, Object upper, boolean lowerInclude, boolean upperInclude) {
        return b -> b instanceof RangeBounds &&
            matchLiteral(lower, ((RangeBounds)b).lowerBound()) &&
            matchLiteral(upper, ((RangeBounds)b).upperBound()) &&
            lowerInclude == ((RangeBounds)b).lowerInclude() &&
            upperInclude == ((RangeBounds)b).upperInclude();
    }

    /** */
    private static Predicate<SearchBounds> multi(Predicate<SearchBounds>... predicates) {
        return b -> b instanceof MultiBounds &&
            ((MultiBounds)b).bounds().size() == predicates.length &&
            matchBounds(((MultiBounds)b).bounds(), predicates);
    }

    /** */
    private static boolean matchLiteral(Object val, RexNode bound) {
        // Compare string representation, since numbers in RexLiteral stored in BigDecimal data type.
        return val == null ? bound == null : bound != null &&
            Objects.toString(val).equals(Objects.toString(RexLiteral.value(bound)));
    }
}
