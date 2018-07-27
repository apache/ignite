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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.jetbrains.annotations.NotNull;

/**
 * Local system view base class (which uses only local node data).
 */
public abstract class SqlAbstractLocalSystemView extends SqlAbstractSystemView {
    /**
     * @param tblName Table name.
     * @param desc Description.
     * @param ctx Context.
     * @param indexes Indexed columns.
     * @param cols Columns.
     */
    public SqlAbstractLocalSystemView(String tblName, String desc, GridKernalContext ctx, String[] indexes,
        Column... cols) {
        super(tblName, desc, ctx, cols, indexes);

        assert tblName != null;
        assert ctx != null;
        assert cols != null;
        assert indexes != null;
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @param data Data for each column.
     */
    protected Row createRow(Session ses, long key, Object... data) {
        Value[] values = new Value[data.length];

        for (int i = 0; i < data.length; i++) {
            Object o = data[i];

            Value v = (o == null) ? ValueNull.INSTANCE :
                (o instanceof Value) ? (Value)o : ValueString.get(o.toString());

            values[i] = cols[i].convert(v);
        }

        Row row = ses.getDatabase().createRow(values, 1);

        row.setKey(key);

        return row;
    }

    /**
     * Gets column index by name.
     *
     * @param colName Column name.
     */
    protected int getColumnIndex(String colName) {
        assert colName != null;

        for (int i = 0; i < cols.length; i++)
            if (colName.equalsIgnoreCase(cols[i].getName()))
                return i;

        return -1;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /**
     * Parse condition for column.
     *
     * @param colName Column name.
     * @param first First.
     * @param last Last.
     */
    protected SqlSystemViewColumnCondition conditionForColumn(String colName, SearchRow first, SearchRow last) {
        return SqlSystemViewColumnCondition.forColumn(getColumnIndex(colName), first, last);
    }

    /**
     * Converts value to UUID safe (suppressing exceptions).
     *
     * @param val UUID.
     */
    protected static UUID uuidFromValue(Value val) {
        try {
            return UUID.fromString(val.getString());
        }
        catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Parent-child Row iterable.
     *
     * @param <P> Parent class.
     * @param <C> Child class
     */
    protected class ParentChildRowIterable<P, C> implements Iterable<Row> {
        /** Session. */
        private final Session ses;

        /** Parent iterable. */
        private final Iterable<P> parents;

        /** Child iterator closure. */
        private final IgniteClosure<P, Iterator<C>> cloChildIter;

        /** Result from parent and child closure. */
        private final IgniteBiClosure<P, C, Object[]> cloRowFromParentChild;

        /**
         * @param ses Session.
         * @param parents Parents.
         * @param cloChildIter Child iterator closure.
         * @param cloRowFromParentChild Row columns from parent and child closure.
         */
        public ParentChildRowIterable(Session ses, Iterable<P> parents,
            IgniteClosure<P, Iterator<C>> cloChildIter,
            IgniteBiClosure<P, C, Object[]> cloRowFromParentChild) {
            this.ses = ses;
            this.parents = parents;
            this.cloChildIter = cloChildIter;
            this.cloRowFromParentChild = cloRowFromParentChild;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Row> iterator() {
            return new ParentChildRowIterator(ses, parents.iterator(), cloChildIter, cloRowFromParentChild);
        }
    }

    /**
     * Parent-child Row iterator.
     *
     * @param <P> Parent class.
     * @param <C> Child class
     */
    protected class ParentChildRowIterator<P, C> extends ParentChildIterator<P, C, Row> {
        /**
         * @param ses
         * @param parentIter Parent iterator.
         * @param cloChildIter
         * @param cloResFromParentChild
         */
        public ParentChildRowIterator(final Session ses, Iterator<P> parentIter,
            IgniteClosure<P, Iterator<C>> cloChildIter,
            final IgniteBiClosure<P, C, Object[]> cloResFromParentChild) {
            super(parentIter, cloChildIter, new IgniteBiClosure<P, C, Row>() {
                /** Row count. */
                private int rowCnt = 0;

                @Override public Row apply(P p, C c) {
                    return SqlAbstractLocalSystemView.this.createRow(ses, ++rowCnt, cloResFromParentChild.apply(p, c));
                }
            });
        }
    }

    /**
     * Parent-child iterator.
     * Lazy 2 levels iterator, which iterates over child items for each parent item.
     *
     * @param <P> Parent class.
     * @param <C> Child class.
     * @param <R> Result item class.
     */
    protected class ParentChildIterator<P, C, R> implements Iterator<R> {
        /** Parent iterator. */
        private final Iterator<P> parentIter;

        /** Child iterator closure. This closure helps to get child iterator for each parent item. */
        private final IgniteClosure<P, Iterator<C>> cloChildIter;

        /**
         * Result from parent and child closure. This closure helps to produce resulting item from parent and child
         * items.
         */
        private final IgniteBiClosure<P, C, R> cloResFromParentChild;

        /** Child iterator. */
        private Iterator<C> childIter;

        /** Next parent. */
        private P nextParent;

        /** Next child. */
        private C nextChild;

        /**
         * @param parentIter Parent iterator.
         */
        public ParentChildIterator(Iterator<P> parentIter,
            IgniteClosure<P, Iterator<C>> cloChildIter,
            IgniteBiClosure<P, C, R> cloResFromParentChild) {

            this.parentIter = parentIter;
            this.cloChildIter = cloChildIter;
            this.cloResFromParentChild = cloResFromParentChild;

            moveChild();
        }

        /**
         * Move to next parent.
         */
        protected void moveParent() {
            nextParent = parentIter.next();

            childIter = cloChildIter.apply(nextParent);
        }

        /**
         * Move to next child.
         */
        protected void moveChild() {
            // First iteration.
            if (nextParent == null && parentIter.hasNext())
                moveParent();

            // Empty parent at first iteration.
            if (childIter == null)
                return;

            while (childIter.hasNext() || parentIter.hasNext()) {
                if (childIter.hasNext()) {
                    nextChild = childIter.next();

                    return;
                }
                else
                    moveParent();
            }

            nextChild = null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextChild != null;
        }

        /** {@inheritDoc} */
        @Override public R next() {
            if (nextChild == null)
                return null;

            R res = cloResFromParentChild.apply(nextParent, nextChild);

            moveChild();

            return res;
        }
    }
}
