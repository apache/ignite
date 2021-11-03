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

package org.apache.ignite.internal.calcite.util;

import static org.apache.ignite.internal.calcite.util.Commons.getAllFromCursor;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.ResultFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.Cursor;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.core.SubstringMatcher;
import org.jetbrains.annotations.Nullable;

/**
 * Query checker.
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
        return containsSubPlan("IgniteIndexScan(table=[[" + schema + ", " + tblName + "]], index=[" + idxName + ']');
    }
    
    /**
     * Ignite table|index scan with projects unmatcher.
     *
     * @param schema  Schema name.
     * @param tblName Table name.
     * @return Matcher.
     */
    public static Matcher<String> notContainsProject(String schema, String tblName) {
        return CoreMatchers.not(containsSubPlan("Scan(table=[[" + schema + ", "
                + tblName + "]], " + "requiredColunms="));
    }
    
    /**
     * {@link #containsProject(String, String, int...)} reverter.
     */
    public static Matcher<String> notContainsProject(String schema, String tblName, int... requiredColunms) {
        return CoreMatchers.not(containsProject(schema, tblName, requiredColunms));
    }
    
    /**
     * Ignite table|index scan with projects matcher.
     *
     * @param schema          Schema name.
     * @param tblName         Table name.
     * @param requiredColunms columns in projection.
     * @return Matcher.
     */
    public static Matcher<String> containsProject(String schema, String tblName, int... requiredColunms) {
        Matcher<String> res = matches(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
                + tblName + "\\]\\], " + ".*requiredColumns=\\[\\{"
                + Arrays.toString(requiredColunms)
                        .replaceAll("\\[", "")
                        .replaceAll("]", "") + "\\}\\].*");
        return res;
    }
    
    /**
     * Ignite table|index scan with only one project matcher.
     *
     * @param schema          Schema name.
     * @param tblName         Table name.
     * @param requiredColunms columns in projection.
     * @return Matcher.
     */
    public static Matcher<String> containsOneProject(String schema, String tblName, int... requiredColunms) {
        return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
                + tblName + "\\]\\], " + ".*requiredColumns=\\[\\{"
                + Arrays.toString(requiredColunms)
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
        return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", "
                + tblName + "\\]\\],.*requiredColumns=\\[\\{(\\d|\\W|,)+\\}\\].*");
    }
    
    /**
     * Sub plan matcher.
     *
     * @param subPlan Subplan.
     * @return Matcher.
     */
    public static Matcher<String> containsSubPlan(String subPlan) {
        return CoreMatchers.containsString(subPlan);
    }
    
    /**
     *
     */
    public static Matcher<String> matches(final String substring) {
        return new SubstringMatcher("contains", false, substring) {
            /** {@inheritDoc} */
            @Override
            protected boolean evalSubstringOf(String strIn) {
                strIn = strIn.replaceAll("\n", "");
                
                return strIn.matches(substring);
            }
        };
    }
    
    /**
     *
     */
    public QueryChecker matches(Matcher<String>... planMatcher) {
        Collections.addAll(planMatchers, planMatcher);
        
        return this;
    }
    
    /** Matches only one occurrence. */
    public static Matcher<String> matchesOnce(final String substring) {
        return new SubstringMatcher("contains once", false, substring) {
            /** {@inheritDoc} */
            @Override
            protected boolean evalSubstringOf(String strIn) {
                strIn = strIn.replaceAll("\n", "");
                
                return containsOnce(strIn, substring);
            }
        };
    }
    
    /** Check only single matching. */
    public static boolean containsOnce(final String s, final CharSequence substring) {
        Pattern pattern = Pattern.compile(substring.toString());
        java.util.regex.Matcher matcher = pattern.matcher(s);
    
        if (matcher.find()) {
            return !matcher.find();
        }
        
        return false;
    }
    
    /**
     * Ignite any index can matcher.
     *
     * @param schema   Schema name.
     * @param tblName  Table name.
     * @param idxNames Index names.
     * @return Matcher.
     */
    public static Matcher<String> containsAnyScan(final String schema, final String tblName, String... idxNames) {
        if (nullOrEmpty(idxNames)) {
            return matchesOnce(".*Ignite(Table|Index)Scan\\(table=\\[\\[" + schema + ", " + tblName + "\\]\\].*");
        }
        
        return CoreMatchers.anyOf(
                Arrays.stream(idxNames).map(idx -> containsIndexScan(schema, tblName, idx)).collect(Collectors.toList())
        );
    }
    
    /**
     *
     */
    private final String qry;
    
    /**
     *
     */
    private final ArrayList<Matcher<String>> planMatchers = new ArrayList<>();
    
    /**
     *
     */
    private List<List<?>> expectedResult;
    
    /**
     *
     */
    private List<String> expectedColumnNames;
    
    /**
     *
     */
    private List<Type> expectedColumnTypes;
    
    /**
     *
     */
    private boolean ordered;
    
    /**
     *
     */
    private Object[] params = OBJECT_EMPTY_ARRAY;
    
    /**
     *
     */
    private String exactPlan;
    
    /**
     *
     */
    public QueryChecker(String qry) {
        this.qry = qry;
    }
    
    /**
     *
     */
    public QueryChecker ordered() {
        ordered = true;
        
        return this;
    }
    
    /**
     *
     */
    public QueryChecker withParams(Object... params) {
        this.params = params;
        
        return this;
    }
    
    /**
     *
     */
    public QueryChecker returns(Object... res) {
        if (expectedResult == null) {
            expectedResult = new ArrayList<>();
        }
        
        expectedResult.add(Arrays.asList(res));
        
        return this;
    }
    
    /**
     *
     */
    public static Matcher<String> containsUnion(boolean all) {
        return CoreMatchers.containsString("IgniteUnionAll(all=[" + all + "])");
    }
    
    /**
     *
     */
    public QueryChecker columnNames(String... columns) {
        expectedColumnNames = Arrays.asList(columns);
        
        return this;
    }
    
    /**
     *
     */
    public QueryChecker columnTypes(Type... columns) {
        expectedColumnTypes = Arrays.asList(columns);
        
        return this;
    }
    
    /**
     *
     */
    public QueryChecker planEquals(String plan) {
        exactPlan = plan;
        
        return this;
    }
    
    /**
     *
     */
    public void check() {
        // Check plan.
        QueryProcessor qryProc = getEngine();
        
        List<SqlCursor<List<?>>> explainCursors =
                qryProc.query("PUBLIC", "EXPLAIN PLAN FOR " + qry);
        
        Cursor<List<?>> explainCursor = explainCursors.get(0);
        List<List<?>> explainRes = getAllFromCursor(explainCursor);
        String actualPlan = (String) explainRes.get(0).get(0);
        
        if (!CollectionUtils.nullOrEmpty(planMatchers)) {
            for (Matcher<String> matcher : planMatchers) {
                assertThat("Invalid plan:\n" + actualPlan, actualPlan, matcher);
            }
        }
    
        if (exactPlan != null) {
            assertEquals(exactPlan, actualPlan);
        }
        
        // Check result.
        List<SqlCursor<List<?>>> cursors =
                qryProc.query("PUBLIC", qry, params);
        
        SqlCursor<List<?>> cur = cursors.get(0);
        
        if (expectedColumnNames != null) {
            List<String> colNames = cur.metadata().fields().stream()
                    .map(ResultFieldMetadata::name)
                    .collect(Collectors.toList());
            
            assertThat("Column names don't match", colNames, equalTo(expectedColumnNames));
        }
        
        if (expectedColumnTypes != null) {
            List<Type> colNames = cur.metadata().fields().stream()
                    .map(ResultFieldMetadata::type)
                    .map(org.apache.ignite.internal.processors.query.calcite.util.Commons::nativeTypeToClass)
                    .collect(Collectors.toList());
            
            assertThat("Column types don't match", colNames, equalTo(expectedColumnTypes));
        }
        
        List<List<?>> res = getAllFromCursor(cur);
        
        if (expectedResult != null) {
            if (!ordered) {
                // Avoid arbitrary order.
                res.sort(new ListComparator());
                expectedResult.sort(new ListComparator());
            }
            
            assertEqualsCollections(expectedResult, res);
        }
    }
    
    /**
     *
     */
    protected abstract QueryProcessor getEngine();
    
    /**
     * Check collections equals (ordered).
     *
     * @param exp Expected collection.
     * @param act Actual collection.
     */
    private void assertEqualsCollections(Collection<?> exp, Collection<?> act) {
        assertEquals(exp.size(), act.size(), "Collections sizes are not equal:\nExpected: " + exp + "\nActual:   " + act);
        
        Iterator<?> it1 = exp.iterator();
        Iterator<?> it2 = act.iterator();
        
        int idx = 0;
        
        while (it1.hasNext()) {
            Object item1 = it1.next();
            Object item2 = it2.next();
    
            if (!eq(item1, item2)) {
                fail("Collections are not equal (position " + idx + "):\nExpected: " + exp + "\nActual:   " + act);
            }
            
            idx++;
        }
    }
    
    /**
     * Tests whether specified arguments are equal, or both {@code null}.
     *
     * @param o1 Object to compare.
     * @param o2 Object to compare.
     * @return Returns {@code true} if the specified arguments are equal, or both {@code null}.
     */
    private static boolean eq(@Nullable Object o1, @Nullable Object o2) {
        return o1 == null ? o2 == null : o2 != null && (o1 == o2 || o1.equals(o2));
    }
    
    /**
     *
     */
    private class ListComparator implements Comparator<List<?>> {
        /**
         * {@inheritDoc}
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public int compare(List<?> o1, List<?> o2) {
            if (o1.size() != o2.size()) {
                fail("Collections are not equal:\nExpected:\t" + o1 + "\nActual:\t" + o2);
            }
            
            Iterator<?> it1 = o1.iterator();
            Iterator<?> it2 = o2.iterator();
            
            while (it1.hasNext()) {
                Object item1 = it1.next();
                Object item2 = it2.next();
    
                if (eq(item1, item2)) {
                    continue;
                }
    
                if (item1 == null) {
                    return 1;
                }
    
                if (item2 == null) {
                    return -1;
                }
    
                if (!(item1 instanceof Comparable) && !(item2 instanceof Comparable)) {
                    continue;
                }
                
                Comparable c1 = (Comparable) item1;
                Comparable c2 = (Comparable) item2;
                
                int c = c1.compareTo(c2);
    
                if (c != 0) {
                    return c;
                }
            }
            
            return 0;
        }
    }
}
