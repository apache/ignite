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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.U;

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
    private final Function<Row, Row> project;

    /** */
    public WindowPartitionFactory(
        ExecutionContext<Row> ctx,
        Window.Group grp,
        RelDataType inputRowType
    ) {
        this.ctx = ctx;
        this.grp = grp;
        this.inputRowType = inputRowType;

        List<RelDataType> aggTypes = Commons.transform(grp.aggCalls, Window.RexWinAggCall::getType);
        rowFactory = ctx.rowHandler().factory(Commons.typeFactory(), aggTypes);
        if (grp.isRows)
            // peer comparator in meaningless in rows frame.
            peerCmp = null;
        else
            peerCmp = ctx.expressionFactory().comparator(grp.collation());

        Map<RexNode, Integer> rexToOrd = new LinkedHashMap<>();
        RelDataType aggInputRowType = mapAggregateInputRowType(grp, inputRowType, rexToOrd);

        project = ctx.expressionFactory().project(U.arrayList(rexToOrd.keySet()), inputRowType);

        funcFactory = new WindowFunctionFactory<>(ctx, grp, project, rexToOrd, aggInputRowType);
    }

    /** {@inheritDoc} */
    @Override public WindowPartition<Row> get() {
        if (funcFactory.isStreamable())
            return new StreamWindowPartition<>(peerCmp, funcFactory, rowFactory);
        else
            return new BufferingWindowPartition<>(peerCmp, funcFactory, rowFactory, ctx, grp, project, inputRowType);
    }

    /** Generates input row type for concrete aggregate call. */
    private RelDataType mapAggregateInputRowType(Window.Group grp, RelDataType inputRowType,
        Map<RexNode, Integer> rexToOrd) {

        List<RelDataTypeField> flds = new ArrayList<>();
        int ord = 0;
        for (Window.RexWinAggCall call : grp.aggCalls) {
            for (int i = 0; i < call.operands.size(); i++) {
                RexNode operand = call.operands.get(i);
                if (rexToOrd.containsKey(operand))
                    continue;

                String name;
                if (operand instanceof RexSlot)
                    name = inputRowType.getFieldNames().get(((RexSlot)operand).getIndex());
                else
                    // Not input ref - generate name.
                    name = "fld$" + i;

                flds.add(new RelDataTypeFieldImpl(name, i, operand.getType()));
                rexToOrd.put(operand, ord++);
            }
        }
        return new RelRecordType(flds);
    }

}
