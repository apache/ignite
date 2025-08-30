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
    private final Supplier<WindowPartition<Row>> supplier;

    /** */
    public WindowPartitionFactory(
        ExecutionContext<Row> ctx,
        Window.Group group,
        List<AggregateCall> calls,
        RelDataType inputRowType,
        boolean streaming
    ) {
        supplier = () -> {
            List<RelDataType> aggTypes = Commons.transform(calls, AggregateCall::getType);
            RowHandler.RowFactory<Row> aggRowFactory = ctx.rowHandler().factory(Commons.typeFactory(), aggTypes);

            Comparator<Row> peerCmp;
            if (group.isRows)
                // peer comparator in meaningless in rows frame.
                peerCmp = null;
            else
                peerCmp = ctx.expressionFactory().comparator(group.collation());

            WindowFunctionFactory<Row> accFactory = new WindowFunctionFactory<>(ctx, group, calls, inputRowType);
            assert !streaming || (streaming && accFactory.isStreamable()) : "Streaming window partition desired, but buffering is required";
            if (accFactory.isStreamable())
                return new StreamWindowPartition<>(peerCmp, accFactory, aggRowFactory);
            else
                return new BufferingWindowPartition<>(peerCmp, accFactory, aggRowFactory, ctx, group, inputRowType);
        };
    }

    /** {@inheritDoc} */
    @Override public WindowPartition<Row> get() {
        return supplier.get();
    }
}
