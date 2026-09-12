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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** Factory to create {@link WindowPartitionBase} factory from {@link Window.Group}. */
public final class WindowPartitionFactory<Row> {
    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    public WindowPartitionFactory(ExecutionContext<Row> ctx) {
        this.ctx = ctx;
    }

    /** Creates streaming window partition for provieded group, aggragates and input row type, */
    public StreamingWindowPartition<Row> newStreamingPartition(Window.Group grp, List<AggregateCall> calls,
        RelDataType inputRowType) {

        assert WindowFunctions.streamable(grp);

        return createPartition(grp, calls, inputRowType, StreamingWindowPartition::new);
    }

    /** Creates buffering window partition for provieded group, aggragates and input row type, */
    public BufferingWindowPartition<Row> newBufferingPartition(Window.Group grp, List<AggregateCall> calls,
        RelDataType inputRowType) {

        return createPartition(grp, calls, inputRowType, (peerCmp, funcFactory, rowFactory)
            -> new BufferingWindowPartition<>(peerCmp, funcFactory, rowFactory, ctx, grp, inputRowType));
    }

    /** */
    private <T extends WindowPartition<Row>> T createPartition(Window.Group grp, List<AggregateCall> calls,
        RelDataType inputRowType, PartitionCreator<Row, T> creator) {
        Comparator<Row> peerCmp;
        if (grp.isRows)
            // peer comparator in meaningless in rows frame.
            peerCmp = null;
        else
            peerCmp = ctx.expressionFactory().comparator(grp.collation());

        WindowFunctionFactory<Row> funcFactory = new WindowFunctionFactory<>(ctx, grp, calls, inputRowType);

        List<RelDataType> aggTypes = Commons.transform(calls, AggregateCall::getType);
        RowHandler.RowFactory<Row> rowFactory = ctx.rowHandler().factory(Commons.typeFactory(), aggTypes);

        return creator.create(peerCmp, funcFactory, rowFactory);
    }

    /** */
    @FunctionalInterface
    private interface PartitionCreator<Row, T extends WindowPartition<?>> {
        /** */
        T create(Comparator<Row> peerCmp, WindowFunctionFactory<Row> funcFactory, RowHandler.RowFactory<Row> rowFactory);
    }

}
