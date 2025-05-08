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
package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class UncollectNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private int requested;

    /** */
    private int waiting;

    /** */
    private final Deque<Row> buf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    private final Function<Row, Iterator<Row>> iterFactory;

    /** */
    private Iterator<Row> iter;

    /**
     * Creates uncollect node with the rows supplier depending on {@code inType}.
     *
     * @param ctx Execution context.
     * @param inType Input row type.
     * @param outType Output row type.
     * @param withOrdinality With ordinality flag.
     */
    public UncollectNode(
        ExecutionContext<Row> ctx,
        RelDataType inType,
        RelDataType outType,
        boolean withOrdinality
    ) {
        super(ctx, outType);

        iterFactory = iteratorFactory(inType, withOrdinality);
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        buf.clear();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        context().execute(this::flush, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        buf.push(row);

        flush();
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        if (isClosed())
            return;

        flush();
    }

    /** */
    private void flush() throws Exception {
        while (requested > 0 && hasNext()) {
            requested--;

            downstream().push(iter.next());
        }

        if (!hasNext()) {
            if (waiting == 0)
                source().request(waiting = IN_BUFFER_SIZE);
            else if (waiting == -1 && requested > 0) {
                requested = 0;

                downstream().end();
            }
        }
    }

    /** */
    private boolean hasNext() {
        while (!buf.isEmpty() && (iter == null || !iter.hasNext()))
            iter = iterFactory.apply(buf.poll());

        return iter != null && iter.hasNext();
    }

    /** */
    private Function<Row, Iterator<Row>> iteratorFactory(RelDataType inType, boolean withOrdinality) {
        return new FlatProductIteratorFactory(inType, withOrdinality);
    }

    /** */
    private class FlatProductIteratorFactory implements Function<Row, Iterator<Row>> {
        /** */
        private final Function<Object[], Row> rowFactory;

        /**
         * We use only one iterator at time, so following objects below can be shared between different iterators to
         * reduce objects allocation.
         */
        private final AtomicInteger ordinality = new AtomicInteger();

        /** Iterators for each field of input row. */
        private final Iterator<?>[] iters;

        /** Current iterator values for each field of input row. */
        private final Object[] curRow;

        /** Input row. */
        private Row inRow;

        /** Next output row. */
        private Row outRow;

        /** */
        public FlatProductIteratorFactory(RelDataType inType, boolean withOrdinality) {
            RowHandler<Row> rowHnd = context().rowHandler();

            int outFldCnt = rowType().getFieldCount();
            int inFldCnt = inType.getFieldCount();

            Function<Object[], Object>[] fldFactories = (Function<Object[], Object>[])new Function[outFldCnt];

            int outFldIdx = 0;

            for (int inFldIdx = 0; inFldIdx < inFldCnt; inFldIdx++) {
                RelDataType inFieldType = inType.getFieldList().get(inFldIdx).getType();

                final int inFldIdx0 = inFldIdx;

                if (inFieldType instanceof MapSqlType) {
                    fldFactories[outFldIdx++] = row -> ((Map.Entry<Object, Object>)row[inFldIdx0]).getKey();
                    fldFactories[outFldIdx++] = row -> ((Map.Entry<Object, Object>)row[inFldIdx0]).getValue();
                }
                else {
                    RelDataType elementType = inFieldType.getComponentType();

                    if (elementType.isStruct()) {
                        for (int elementFldIdx = 0; elementFldIdx < elementType.getFieldCount(); elementFldIdx++) {
                            final int elementFldIdx0 = elementFldIdx;

                            fldFactories[outFldIdx++] = row -> rowHnd.get(elementFldIdx0, (Row)(row[inFldIdx0]));
                        }
                    }
                    else
                        fldFactories[outFldIdx++] = row -> row[inFldIdx0];
                }
            }

            if (withOrdinality)
                fldFactories[outFldIdx++] = row -> ordinality.incrementAndGet();

            assert outFldIdx == outFldCnt :
                "Unexpected fields count [outFldIdx=" + outFldIdx + ", outFldCnt = " + outFldCnt;

            RowHandler.RowFactory<Row> outRowFactory = rowHnd.factory(context().getTypeFactory(), rowType());

            rowFactory = inRow -> {
                Object[] outRow = new Object[outFldCnt];

                for (int fldIdx = 0; fldIdx < outFldCnt; fldIdx++)
                    outRow[fldIdx] = fldFactories[fldIdx].apply(inRow);

                return outRowFactory.create(outRow);
            };

            iters = (Iterator<?>[])new Iterator[inFldCnt];
            curRow = new Object[inFldCnt];
        }

        /** {@inheritDoc} */
        @Override public Iterator<Row> apply(Row row) {
            ordinality.set(0);

            RowHandler<Row> rowHnd = context().rowHandler();
            int colCnt = rowHnd.columnCount(row);

            inRow = row;

            for (int i = 0; i < colCnt; i++) {
                iters[i] = iterator(rowHnd.get(i, row));

                if (!iters[i].hasNext())
                    return Collections.emptyIterator();

                curRow[i] = iters[i].next();
            }

            outRow = rowFactory.apply(curRow);

            return new Iterator<Row>() {
                @Override public boolean hasNext() {
                    return outRow != null;
                }

                @Override public Row next() {
                    Row res = outRow;

                    if (res == null)
                        throw new NoSuchElementException();

                    advance();

                    return res;
                }
            };
        }

        /** */
        private void advance() {
            RowHandler<Row> rowHnd = context().rowHandler();

            for (int i = iters.length - 1; i >= 0; i--) {
                if (iters[i].hasNext()) {
                    curRow[i] = iters[i].next();

                    break;
                }
                else if (i == 0) {
                    outRow = null;

                    return;
                }
                else {
                    iters[i] = iterator(rowHnd.get(i, inRow));

                    assert iters[i].hasNext();

                    curRow[i] = iters[i].next();
                }
            }

            outRow = rowFactory.apply(curRow);
        }

        /** */
        private Iterator<Object> iterator(Object iterable) {
            if (iterable == null)
                return Collections.emptyIterator();

            assert iterable instanceof Map || iterable instanceof Iterable :
                "Unexpected iterable class: " + iterable.getClass();

            if (iterable instanceof Map)
                iterable = ((Map<Object, Object>)iterable).entrySet();

            return ((Iterable<Object>)iterable).iterator();
        }
    }
}
