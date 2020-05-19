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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.calcite.rel.core.TableModify;
import org.apache.ignite.IgniteCheckedException;
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
public class ModifyNode<Row, K, V> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    protected final TableDescriptor<K, V, Row> desc;

    /** */
    private final TableModify.Operation op;

    /** */
    private final List<String> cols;

    /** */
    private List<IgniteBiTuple<K, V>> tuples = new ArrayList<>(MODIFY_BATCH_SIZE);

    /** */
    private long updatedRows;

    /** */
    private int waiting;

    /** */
    private int requested;

    /** */
    private boolean inLoop;

    /** */
    private State state = State.UPDATING;

    /**
     * @param ctx Execution context.
     * @param desc Table descriptor.
     * @param cols Update column list.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ModifyNode(
        ExecutionContext<Row> ctx,
        TableDescriptor<K, V, Row> desc,
        TableModify.Operation op,
        List<String> cols
    ) {
        super(ctx);

        this.desc = desc;
        this.op = op;
        this.cols = cols;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (!inLoop)
            tryEnd();
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        checkThread();

        assert downstream != null;
        assert waiting > 0;
        assert state == State.UPDATING;

        waiting--;

        try {
            switch (op) {
                case DELETE:
                case UPDATE:
                case INSERT:
                    addToBatch(row);

                    break;
                default:
                    throw new UnsupportedOperationException(op.name());
            }

            if (waiting == 0)
                F.first(sources).request(waiting = MODIFY_BATCH_SIZE);
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting = -1;
        state = State.UPDATED;

        tryEnd();
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    private void addToBatch(Row row) throws IgniteCheckedException {
        tuples.add(desc.toTuple(context(), row, op, cols));

        flush(false);
    }

    /** */
    private void tryEnd() {
        assert downstream != null;

        inLoop = true;
        try {
            if (state == State.UPDATING && waiting == 0)
                F.first(sources).request(waiting = MODIFY_BATCH_SIZE);

            if (state == State.UPDATED && requested > 0) {
                flush(true);

                state = State.END;

                requested--;
                downstream.push(hnd.create(updatedRows));
            }

            if (state == State.END && requested > 0) {
                downstream.end();
                requested = 0;
            }
        }
        catch (Exception e) {
            downstream.onError(e);
        }
        finally {
            inLoop = false;
        }
    }

    /** */
    private void flush(boolean force) throws IgniteCheckedException {
        if (F.isEmpty(tuples) || !force && tuples.size() < MODIFY_BATCH_SIZE)
            return;

        List<IgniteBiTuple<K, V>> tuples = this.tuples;
        this.tuples = new ArrayList<>(MODIFY_BATCH_SIZE);

        Map<K, EntryProcessorResult<Long>> res =
            desc.cacheContext().cache().invokeAll(invokeMap(tuples));

        long updated = res.values().stream().mapToLong(EntryProcessorResult::get).sum();

        if (op == TableModify.Operation.INSERT && updated != res.size()) {
            List<Object> duplicates = new ArrayList<>(res.size());

            for (Map.Entry<K, EntryProcessorResult<Long>> e : res.entrySet()) {
                if (e.getValue().get() == 0)
                    duplicates.add(e.getKey());
            }

            throw duplicateKeysException(duplicates);
        }

        updatedRows += updated;
    }

    /** */
    private Map<K, EntryProcessor<K, V, Long>> invokeMap(Collection<IgniteBiTuple<K, V>> tuples) {
        Map<K, EntryProcessor<K, V, Long>> procMap = U.newLinkedHashMap(tuples.size());

        switch (op) {
            case INSERT:
                for (IgniteBiTuple<K, V> entry : tuples)
                    procMap.put(entry.getKey(), new InsertOperation<>(entry.getValue()));

                break;
            case UPDATE:
                for (IgniteBiTuple<K, V> entry : tuples)
                    procMap.put(entry.getKey(), new UpdateOperation<>(entry.getValue()));

                break;
            case DELETE:
                for (IgniteBiTuple<K, V> entry : tuples)
                    procMap.put(entry.getKey(), new DeleteOperation<>());

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
    private enum State {
        /** */
        UPDATING,

        /** */
        UPDATED,

        /** */
        END
    }

    /** */
    private static class InsertOperation<K, V> implements EntryProcessor<K, V, Long> {
        /** */
        private final V val;

        /** */
        private InsertOperation(V val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            if (!entry.exists()) {
                entry.setValue(val);

                return 1L;
            }

            return 0L;
        }
    }

    /** */
    private static class UpdateOperation<K, V> implements EntryProcessor<K, V, Long> {
        /** */
        private final V val;

        /** */
        private UpdateOperation(V val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            if (entry.exists()) {
                entry.setValue(val);

                return 1L;
            }

            return 0L;
        }
    }

    /** */
    private static class DeleteOperation<K, V> implements EntryProcessor<K, V, Long> {
        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            if (entry.exists()) {
                entry.remove();

                return 1L;
            }

            return 0L;
        }
    }
}
