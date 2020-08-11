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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *  Query checker.
 */
public abstract class QueryChecker {
    /**
     * Ignite table scan matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> containsScan(String schema, String tblName) {
        return containsSubPlan("IgniteTableScan(table=[[" + schema + ", " + tblName + "]]");
    }

    /**
     * Ignite index scan matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @return Matcher.
     */
    public static Matcher<String> containsScan(String schema, String tblName, String idxName) {
        return containsSubPlan("IgniteTableScan(table=[[" + schema + ", " + tblName + "]], index=[" + idxName + ']');
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

    /**
     * Ignite any index can matcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @param idxNames Index names.
     * @return Matcher.
     */
    public static Matcher<String> containsAnyScan(final String schema, final String tblName, String... idxNames) {
        return CoreMatchers.anyOf(
            Arrays.stream(idxNames).map(idx -> CoreMatchers.containsString(
                "IgniteTableScan(table=[[" + schema + ", " + tblName + "]], index=[" + idx + ']'
            )).collect(Collectors.toList()));
    }

    /** */
    private final String qry;

    /** */
    private final ArrayList<Matcher<String>> planMatchers = new ArrayList<>();

    /** */
    private List<List<?>> expectedResult = null;

    /** */
    private boolean ordered;

    /** */
    private Object[] params = X.EMPTY_OBJECT_ARRAY;

    /** */
    private String exactPlan;

    /** */
    public QueryChecker(String qry) {
        this.qry = qry;
    }

    /** */
    public QueryChecker ordered() {
        ordered = true;

        return this;
    }

    /** */
    public QueryChecker withParams(Object... params) {
        this.params = params;

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
    public static Matcher<String> containsUnion(boolean all) {
        return CoreMatchers.containsString("IgniteUnionAll(all=[" + all + "])");
    }

    /** */
    public QueryChecker and(Matcher<String> condition) {
        planMatchers.add(condition);
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

        List<FieldsQueryCursor<List<?>>> explainCursors =
            engine.query(null, "PUBLIC", "EXPLAIN PLAN FOR " + qry);

        FieldsQueryCursor<List<?>> explainCursor = explainCursors.get(0);
        List<List<?>> explainRes = explainCursor.getAll();
        String actualPlan = (String)explainRes.get(0).get(0);

        if (!F.isEmpty(planMatchers)) {
            Matcher<String> matcher = CoreMatchers.allOf(planMatchers.toArray(new Matcher[planMatchers.size()]));

            if (!matcher.matches(actualPlan)) {
                final StringDescription desc = new StringDescription();

                matcher.describeTo(desc);

                fail(desc.toString());
            }
        }

        if (exactPlan != null) {
            assertEquals(exactPlan, actualPlan);
        }

        // Check result.
        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC", qry, params);

        FieldsQueryCursor<List<?>> cur = cursors.get(0);

        List<List<?>> res = cur.getAll();

        if (expectedResult != null) {
            if (!ordered) {
                // Avoid arbitrary order.
                res.sort(new ListComparator());
                expectedResult.sort(new ListComparator());
            }

            assertEqualsCollections(expectedResult, res);
        }
    }

    protected abstract QueryEngine getEngine();

    /**
     * Check collections equals (ordered).
     * @param exp Expected collection.
     * @param act Actual collection.
     */
    private void assertEqualsCollections(Collection<?> exp, Collection<?> act) {
        assertEquals("Collections sizes are not equal:", exp.size(), act.size());

        Iterator<?> it1 = exp.iterator();
        Iterator<?> it2 = act.iterator();

        int idx = 0;

        while (it1.hasNext()) {
            Object item1 = it1.next();
            Object item2 = it2.next();

            if (!F.eq(item1, item2))
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

                if (F.eq(item1, item2))
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
}
