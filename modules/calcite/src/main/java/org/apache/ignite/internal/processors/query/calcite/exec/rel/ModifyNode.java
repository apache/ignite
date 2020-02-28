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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.calcite.rel.core.TableModify;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;

/**
 *
 */
public class ModifyNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Sink<Object[]> {
    /** */
    private static final int BATCH_SIZE = 100;

    /** */
    protected final TableDescriptor desc;

    /** */
    private final TableModify.Operation op;

    /** */
    private final List<String> columns;

    /** */
    private final Map<Object,Object> tuples = U.newLinkedHashMap(BATCH_SIZE);

    /** */
    private long updatedRows;

    /** */
    private State state = State.UPDATING;

    /**
     * @param ctx Execution context.
     * @param desc Table descriptor.
     * @param columns Update column list.
     * @param input Input node.
     */
    public ModifyNode(ExecutionContext ctx, TableDescriptor desc, TableModify.Operation op, List<String> columns, Node<Object[]> input) {
        super(ctx, input);

        this.desc = desc;
        this.op = op;
        this.columns = columns;

        link();
    }

    /** {@inheritDoc} */
    @Override public Sink<Object[]> sink(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request() {
        checkThread();

        if (state == State.UPDATING)
            input().request();
        else if (state == State.UPDATED)
            endInternal();
        else
            assert state == State.END;
    }

    /** {@inheritDoc} */
    @Override public boolean push(Object[] row) {
        if (state != State.UPDATING)
            return false;

        switch (op) {
            case DELETE:
            case UPDATE:
            case INSERT:
                addToBatch(row);

                break;
            default:
                throw new UnsupportedOperationException(op.name());
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void end() {
        state = State.UPDATED;

        endInternal();
    }

    /** */
    private void addToBatch(Object[] row) {
        try {
            IgniteBiTuple<?, ?> t = desc.toTuple(context(), row, op, columns);
            tuples.put(t.getKey(), t.getValue());
            flush(false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void flush(boolean force) {
        if (F.isEmpty(tuples) || !force && tuples.size() < BATCH_SIZE)
            return;

        try {
            Map<Object, EntryProcessorResult<Long>> res =
                ((GridCacheAdapter) desc.cacheContext().cache()).invokeAll(invokeMap());

            long updated = res.values().stream().mapToLong(EntryProcessorResult::get).sum();

            if (op == TableModify.Operation.INSERT && updated != res.size()) {
                List<Object> duplicates = new ArrayList<>(res.size());

                for (Map.Entry<Object, EntryProcessorResult<Long>> e : res.entrySet()) {
                    if (e.getValue().get() == 0)
                        duplicates.add(e.getKey());
                }

                throw duplicateKeysException(duplicates);
            }

            updatedRows += updated;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        tuples.clear();
    }

    /** */
    private Map<Object, EntryProcessor<Object, Object, Long>> invokeMap() {
        Map<Object, EntryProcessor<Object, Object, Long>> procMap = U.newLinkedHashMap(BATCH_SIZE);

        switch (op) {
            case INSERT:
                for (Map.Entry<Object, Object> entry : tuples.entrySet())
                    procMap.put(entry.getKey(), new InsertOperation(entry.getValue()));

                break;
            case UPDATE:
                for (Map.Entry<Object, Object> entry : tuples.entrySet())
                    procMap.put(entry.getKey(), new UpdateOperation(entry.getValue()));

                break;
            case DELETE:
                for (Map.Entry<Object, Object> entry : tuples.entrySet())
                    procMap.put(entry.getKey(), new DeleteOperation());

                break;
            default:
                throw new AssertionError();
        }

        return procMap;
    }

    /** */
    private IgniteSQLException duplicateKeysException(List<Object> keys) {
        return new IgniteSQLException("Failed to INSERT some keys because they are already in cache [keys=" +
            keys + ']', DUPLICATE_KEY);
    }

    /** */
    private void endInternal() {
        flush(true);

        if (target().push(new Object[]{updatedRows})) {
            state = State.END;
            target().end();
        }
    }

    /** */
    private enum State {
        UPDATING, UPDATED, END
    }

    /** */
    private static class InsertOperation implements EntryProcessor<Object, Object, Long> {
        /** */
        private final Object val;

        /** */
        private InsertOperation(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            if (!entry.exists()) {
                entry.setValue(val);

                return 1L;
            }

            return 0L;
        }
    }

    /** */
    private static class UpdateOperation implements EntryProcessor<Object, Object, Long> {
        /** */
        private final Object val;

        /** */
        private UpdateOperation(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            if (entry.exists()) {
                entry.setValue(val);

                return 1L;
            }

            return 0L;
        }
    }

    /** */
    private static class DeleteOperation implements EntryProcessor<Object, Object, Long> {
        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            if (entry.exists()) {
                entry.remove();

                return 1L;
            }

            return 0L;
        }
    }
}
