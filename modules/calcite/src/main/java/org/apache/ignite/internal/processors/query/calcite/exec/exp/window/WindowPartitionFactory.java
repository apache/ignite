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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** Factory to create {@link WindowPartitionBase} factory from {@link Window.Group}. */
public final class WindowPartitionFactory<Row> implements Supplier<WindowPartition<Row>> {
    /** */
    private final WindowFunctionFactory<Row> funcFactory;

    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    private final Window.Group grp;

    /** */
    private final RelDataType inputRowType;

    /** */
    private final RowHandler.RowFactory<Row> rowFactory;

    /** */
    private final Comparator<Row> peerCmp;

    /** */
    public WindowPartitionFactory(
        ExecutionContext<Row> ctx,
        Window.Group grp,
        List<AggregateCall> calls,
        RelDataType inputRowType
    ) {
        this.ctx = ctx;
        this.grp = grp;
        this.inputRowType = inputRowType;

        List<RelDataType> aggTypes = Commons.transform(calls, AggregateCall::getType);
        rowFactory = ctx.rowHandler().factory(Commons.typeFactory(), aggTypes);
        if (grp.isRows)
            // peer comparator in meaningless in rows frame.
            peerCmp = null;
        else
            peerCmp = ctx.expressionFactory().comparator(grp.collation());

        funcFactory = new WindowFunctionFactory<>(ctx, grp, calls, inputRowType);
    }

    /** {@inheritDoc} */
    @Override public WindowPartition<Row> get() {
        if (funcFactory.isStreamable())
            return new StreamWindowPartition<>(peerCmp, funcFactory, rowFactory);
        else
            return new BufferingWindowPartition<>(peerCmp, funcFactory, rowFactory, ctx, grp, inputRowType);
    }
}
