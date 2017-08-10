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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueString;
import org.jetbrains.annotations.Nullable;

/**
 * Test to check correct work of {@link GridMergeIndexesIterator}.
 * Note that it checks transparent iteration over data in all indexes while being agnostic to
 * what's happening inside {@link GridMergeIndex} implementations themselves - it goes along well with how
 * this iterator is used in {@link GridReduceQueryExecutor}.
 */
@SuppressWarnings("unchecked")
public class GridMergeIndexesIteratorSelfTest extends GridCommonAbstractTest {
    /**
     * Test no data cases.
     */
    public void testEmpty() {
        doTest();

        doTest(e());

        doTest(e(), e(), e());
    }

    /**
     * Test simple case.
     */
    public void testSimple() {
        doTest(l("A"), l("B", "C", "D"), l("E", "F"));
    }

    /**
     * Test complex case with few non-empty item lists with some empty ones in between.
     */
    public void testEmptyCombinations() {
        doTest(e(), l("A"), e(), e(), l("B", "C", "D"), e(), l("E", "F"), e(), e(), e(), l("E", "F"), e());
    }

    /**
     * Test that contents of iterator build on top of indexes built on top of given sets of items
     * match those of an iterator created from list containing all items.
     * @param data data to fill iterator.
     */
    private void doTest(List<String>... data) {
        List<String> exList = new ArrayList<>();

        List<GridMergeIndex> idxs = new ArrayList<>(data.length);

        for (List<String> aData : data) {
            exList.addAll(aData);

            idxs.add(new Index(aData));
        }

        GridMergeIndexesIterator it;

        try {
            it = new GridMergeIndexesIterator(idxs, null);
        }
        catch (IgniteCheckedException e) {
            throw new AssertionError(e);
        }

        Iterator<String> exIt = exList.iterator();

        while (it.hasNext() && exIt.hasNext())
            assertEquals(exIt.next(), it.next().get(0));

        assertFalse(it.hasNext());

        assertFalse(exIt.hasNext());
    }

    /**
     * @param items Items to wrap into list.
     * @return List of {@code items}.
     */
    private static List<String> l(String... items) {
        return Arrays.asList(items);
    }

    /**
     * @return Empty list.
     */
    private static List<String> e() {
        return Collections.emptyList();
    }

    /**
     * Dummy index.
     */
    private final static class Index extends GridMergeIndex {
        /** Cursor. */
        private final Cursor cur;

        /**
         * Constructor.
         * @param items items.
         */
        protected Index(Iterable<String> items) {
            super(null);

            cur = new Cursor(items);
        }

        /** {@inheritDoc} */
        @Override protected void addPage0(GridResultPage page) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean fetchedAll() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override protected org.h2.index.Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last) {
            return cur;
        }

        /** {@inheritDoc} */
        @Override protected org.h2.index.Cursor findAllFetched(List<Row> fetched, @Nullable SearchRow first,
            @Nullable SearchRow last) {
            return cur;
        }

        /** {@inheritDoc} */
        @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, HashSet<Column> allColsSet) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Dummy cursor.
     */
    private final static class Cursor implements org.h2.index.Cursor {
        /** Current item. */
        private String cur;

        /** Items iterator. */
        private final Iterator<String> it;

        /**
         * Constructor.
         * @param items items.
         */
        Cursor(Iterable<String> items) {
            it = items.iterator();
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return new RowImpl(new Value[] { ValueString.get(cur) }, 0);
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (!it.hasNext())
                return false;

            cur = it.next();

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw new UnsupportedOperationException();
        }
    }

}
