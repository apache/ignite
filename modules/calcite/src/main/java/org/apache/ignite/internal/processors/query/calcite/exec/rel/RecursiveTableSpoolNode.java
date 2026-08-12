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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/** Pass-through spool that atomically replaces the recursive CTE delta on end-of-input. */
public class RecursiveTableSpoolNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** Query-local recursive state. */
    private final RecursiveCteState<Row> state;

    /** Whether the current input cycle has started writing. */
    private boolean writing;

    /** */
    public RecursiveTableSpoolNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        RecursiveCteState<Row> state
    ) {
        super(ctx, rowType);

        this.state = state;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        checkState();

        if (!writing) {
            state.beginWrite();
            writing = true;
        }

        source().request(rowsCnt);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert writing;

        checkState();

        state.add(row);
        downstream().push(row);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert writing;

        checkState();

        state.commit();
        writing = false;

        downstream().end();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        writing = false;
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }
}
