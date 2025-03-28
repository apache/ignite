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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTransactionalTest.SqlTransactionMode;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.hamcrest.CoreMatchers;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.core.SubstringMatcher;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *  Query checker.
 */
public abstract class QueryChecker {
    /** Partition release timeout. */
    private static final long PART_RELEASE_TIMEOUT = 5_000L;

    /**
     * Ignite table scan matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> containsTableScan(String schema, String tblName) {
        return containsSubPlan("IgniteTableScan(table=[[" + schema + ", " + tblName + "]]");
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> containsIndexScan(String schema, String tblName) {
        return containsSubPlan("IgniteIndexScan(table=[[" + schema + ", " + tblName + "]]");
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @return Matcher.
     */
    public static Matcher<String> containsIndexScan(String schema, String tblName, String idxName) {
        return CoreMatchers.anyOf(
            containsSubPlan("IgniteIndexScan(table=[[" + schema + ", " + tblName + "]], index=[" + idxName + ']'),
            containsSubPlan("IgniteIndexScan(table=[[" + schema + ", " + tblName + "]], index=[" +
                SchemaManager.generateProxyIdxName(idxName) + ']')
        );
    }

    /**
     * Ignite result row count matсher.
     *
     * @param rowCount Expected result row count.
     * @return Mather.
     */
    public static Matcher<String> containsResultRowCount(double rowCount) {
        String rowCntStr = String.format(".*rowcount = %s,.*", rowCount);

        return new RegexpMather(rowCntStr);
    }

    /**
     * Regexp string matсher.
     */
    private static class RegexpMather extends CustomTypeSafeMatcher<String> {
        /** Compilled pathern. */
        private Pattern pattern;

        /**
         * Constructor.
         *
         * @param regexp Regexp to search.
         */
        public RegexpMather(String regexp) {
            super(regexp);

            pattern = Pattern.compile(regexp, Pattern.DOTALL);
        }

        /** {@inheritDoc} */
        @Override protected boolean matchesSafely(String item) {
            java.util.regex.Matcher matcher = pattern.matcher(item);

            return matcher.matches();
        }
    }

    /**
     * Ignite table|index scan with projects unmatcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> notContainsProject(String schema, String tblName) {
        return CoreMatchers.not(containsSubPlan("Scan(table=[[" + schema + ", " +
            tblName + "]], " + "requiredColunms="));
    }

    /**
     * {@link #containsProject(java.lang.String, java.lang.String, int...)} reverter.
     */
    public static Matcher<String> notContainsProject(String schema, String tblName, int... requiredColunms) {
        return CoreMatchers.not(containsProject(schema, tblName, requiredColunms));
    }

    /**
     * Ignite table|index scan with projects matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @param requiredColunms columns in projection.
     * @return Matcher.
     */
    public static Matcher<String> containsProject(String schema, String tblName, int... requiredColunms) {
        Matcher<String> res = matches(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", " +
            tblName + "\\]\\], " + ".*requiredColumns=\\[\\{" +
            Arrays.toString(requiredColunms)
                .replaceAll("\\[", "")
                .replaceAll("]", "") + "\\}\\].*");
        return res;
    }

    /**
     * Ignite table|index scan with only one project matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @param requiredColunms columns in projection.
     * @return Matcher.
     */
    public static Matcher<String> containsOneProject(String schema, String tblName, int... requiredColunms) {
        return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", " +
            tblName + "\\]\\], " + ".*requiredColumns=\\[\\{" +
            Arrays.toString(requiredColunms)
                .replaceAll("\\[", "")
                .replaceAll("]", "") + "\\}\\].*");
    }

    /**
     * Ignite table|index scan with any project matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> containsAnyProject(String schema, String tblName) {
        return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", " +
            tblName + "\\]\\],.*requiredColumns=\\[\\{(\\d|\\W|,)+\\}\\].*");
    }

    /**
     * Sub plan matcher.
     *
     * @param subPlan  Subplan.
     * @return Matcher.
     */
    public static Matcher<String> containsSubPlan(String subPlan) {
        return CoreMatchers.containsString(subPlan);
    }

    /** */
    public static Matcher<String> matches(final String substring) {
        return new SubstringMatcher(substring) {
            /** {@inheritDoc} */
            @Override protected boolean evalSubstringOf(String sIn) {
                sIn = sIn.replaceAll("\n", "");

                return sIn.matches(substring);
            }

            /** {@inheritDoc} */
            @Override protected String relationship() {
                return null;
            }
        };
    }

    /** Matches only one occurance. */
    public static Matcher<String> matchesOnce(final String substring) {
        return new SubstringMatcher(substring) {
            /** {@inheritDoc} */
            @Override protected boolean evalSubstringOf(String sIn) {
                sIn = sIn.replaceAll("\n", "");

                return containsOnce(sIn, substring);
            }

            /** {@inheritDoc} */
            @Override protected String relationship() {
                return null;
            }
        };
    }

    /** Check only single matching. */
    public static boolean containsOnce(final String s, final CharSequence substring) {
        Pattern pattern = Pattern.compile(substring.toString());
        java.util.regex.Matcher matcher = pattern.matcher(s);

        if (matcher.find())
            return !matcher.find();

        return false;
    }

    /**
     * Ignite any index can matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @param idxNames Index names.
     * @return Matcher.
     */
    public static Matcher<String> containsAnyScan(final String schema, final String tblName, String... idxNames) {
        if (F.isEmpty(idxNames))
            return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\].*");

        return CoreMatchers.anyOf(
            Arrays.stream(idxNames).map(idx -> containsIndexScan(schema, tblName, idx)).collect(Collectors.toList())
        );
    }

    /** */
    private final String qry;

    /** */
    private final Transaction tx;

    /** */
    private final ArrayList<Matcher<String>> planMatchers = new ArrayList<>();

    /** */
    private List<List<?>> expectedResult;

    /** */
    private int expectedResultSize = -1;

    /** */
    private List<String> expectedColumnNames;

    /** */
    private boolean ordered;

    /** */
    private boolean withRowsIterator;

    /** */
    private Object[] params = X.EMPTY_OBJECT_ARRAY;

    /** */
    private String exactPlan;

    /** */
    private FrameworkConfig frameworkCfg;

    /** */
    public QueryChecker(String qry) {
        this(qry, null, SqlTransactionMode.NONE);
    }

    /** */
    public QueryChecker(String qry, Transaction tx, SqlTransactionMode sqlTxMode) {
        assert (tx != null && sqlTxMode != SqlTransactionMode.NONE)
            || (tx == null && sqlTxMode == SqlTransactionMode.NONE) : "mode = " + sqlTxMode + ", tx = " + tx;

        this.qry = qry;
        this.tx = tx;
    }

    /** */
    public QueryChecker ordered() {
        ordered = true;

        return this;
    }

    /** */
    public QueryChecker withRowsIterator(boolean flag) {
        withRowsIterator = flag;

        return this;
    }

    /** */
    public QueryChecker withParams(Object... params) {
        this.params = params;

        return this;
    }

    /** */
    public QueryChecker withFrameworkConfig(FrameworkConfig frameworkCfg) {
        this.frameworkCfg = frameworkCfg;

        return this;
    }

    /** */
    public QueryChecker returns(Object... res) {
        if (expectedResult == null)
            expectedResult = new ArrayList<>();

        expectedResult.add(Arrays.asList(res));

        return this;
    }

    /** */
    public QueryChecker resultSize(int size) {
        expectedResultSize = size;

        return this;
    }

    /** */
    public static Matcher<String> containsUnion(boolean all) {
        return CoreMatchers.containsString("IgniteUnionAll(all=[" + all + "])");
    }

    /** */
    public QueryChecker columnNames(String... columns) {
        expectedColumnNames = Arrays.asList(columns);

        return this;
    }

    /** */
    public QueryChecker matches(Matcher<String>... planMatcher) {
        Collections.addAll(planMatchers, planMatcher);

        return this;
    }

    /** */
    public QueryChecker planEquals(String plan) {
        exactPlan = plan;

        return this;
    }

    /** */
    public void check() {
        // Check plan.
        QueryEngine engine = getEngine();

        GridCacheVersion txVer = tx != null
            ? ((TransactionProxyImpl)tx).tx().xidVersion()
            : null;

        QueryContext ctx = (frameworkCfg != null || txVer != null) ? QueryContext.of(frameworkCfg, txVer) : null;

        List<FieldsQueryCursor<List<?>>> explainCursors =
            engine.query(ctx, "PUBLIC", "EXPLAIN PLAN FOR " + qry, params);

        FieldsQueryCursor<List<?>> explainCursor = explainCursors.get(0);
        List<List<?>> explainRes = explainCursor.getAll();
        String actualPlan = (String)explainRes.get(0).get(0);

        // Will not check plan in transaction, because, statistic not refreshed inside transaction, so plan differs from expected.
        if (!F.isEmpty(planMatchers) && tx == null) {
            for (Matcher<String> matcher : planMatchers)
                assertThat("Invalid plan:\n" + actualPlan + "\n for query: " + qry, actualPlan, matcher);
        }

        if (exactPlan != null)
            assertEquals(exactPlan, actualPlan);

        // Check result.
        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(ctx, "PUBLIC", qry, params);

        FieldsQueryCursor<List<?>> cur = cursors.get(0);

        if (expectedColumnNames != null) {
            List<String> colNames = IntStream.range(0, cur.getColumnsCount())
                .mapToObj(cur::getFieldName).collect(Collectors.toList());

            assertThat("Column names don't match", colNames, equalTo(expectedColumnNames));
        }

        List<List<?>> res;
        if (withRowsIterator) {
            res = new ArrayList<>();
            for (Iterator<List<?>> it = cur.iterator(); it.hasNext(); )
                res.add(it.next());
        }
        else
            res = cur.getAll();

        if (expectedResultSize >= 0)
            assertEquals("Unexpected result size", expectedResultSize, res.size());

        if (expectedResult != null) {
            if (!ordered) {
                // Avoid arbitrary order.
                res.sort(new ListComparator());
                expectedResult.sort(new ListComparator());
            }

            assertEqualsCollections(expectedResult, res);
        }
    }

    /** */
    protected abstract QueryEngine getEngine();

    /**
     * Check collections equals (ordered).
     * @param exp Expected collection.
     * @param act Actual collection.
     */
    private void assertEqualsCollections(Collection<?> exp, Collection<?> act) {
        assertEquals("Collections sizes are not equal:\nExpected: " + exp + "\nActual:   " + act, exp.size(), act.size());

        Iterator<?> it1 = exp.iterator();
        Iterator<?> it2 = act.iterator();

        int idx = 0;

        while (it1.hasNext()) {
            Object item1 = it1.next();
            Object item2 = it2.next();

            if (!Objects.equals(item1, item2))
                fail("Collections are not equal (position " + idx + "):\nExpected: " + exp + "\nActual:   " + act);

            idx++;
        }
    }

    /**
     *
     */
    private class ListComparator implements Comparator<List<?>> {
        /**
         * {@inheritDoc}
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override public int compare(List<?> o1, List<?> o2) {
            if (o1.size() != o2.size())
                fail("Collections are not equal:\nExpected:\t" + o1 + "\nActual:\t" + o2);

            Iterator<?> it1 = o1.iterator();
            Iterator<?> it2 = o2.iterator();

            while (it1.hasNext()) {
                Object item1 = it1.next();
                Object item2 = it2.next();

                if (Objects.equals(item1, item2))
                    continue;

                if (item1 == null)
                    return 1;

                if (item2 == null)
                    return -1;

                if (!(item1 instanceof Comparable) && !(item2 instanceof Comparable))
                    continue;

                Comparable c1 = (Comparable)item1;
                Comparable c2 = (Comparable)item2;

                int c = c1.compareTo(c2);

                if (c != 0)
                    return c;
            }

            return 0;
        }
    }

    /**
     * @param cacheName Cache to check
     * @throws IgniteInterruptedCheckedException
     */
    public static void awaitReservationsRelease(String cacheName) throws IgniteInterruptedCheckedException {
        for (Ignite ign : G.allGrids())
            awaitReservationsRelease((IgniteEx)ign, cacheName);
    }

    /**
     * @param node Node to check reservation.
     * @param cacheName Cache to check reservations.
     */
    public static void awaitReservationsRelease(IgniteEx node, String cacheName) throws IgniteInterruptedCheckedException {
        GridDhtCacheAdapter c = GridTestUtils.getFieldValue(node.cachex(cacheName), "delegate");

        List<GridDhtLocalPartition> parts = c.topology().localPartitions();

        GridTestUtils.waitForCondition(() -> {
            for (GridDhtLocalPartition p : parts) {
                if (p.reservations() > 0)
                    return false;
            }

            return true;
        }, PART_RELEASE_TIMEOUT);

        for (GridDhtLocalPartition p : parts)
            assertEquals("Partition is reserved: [node=" + node.name() + ", part=" + p, 0, p.reservations());
    }
}
