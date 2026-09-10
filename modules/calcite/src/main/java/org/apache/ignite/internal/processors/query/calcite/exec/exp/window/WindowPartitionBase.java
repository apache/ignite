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
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.jetbrains.annotations.Nullable;

/** Base implementation of window partition. */
abstract class WindowPartitionBase<Row> implements WindowPartition<Row> {
    /** Comparator for computing the peer index. */
    private final Comparator<Row> peerCmp;

    /** */
    private final WindowFunctionFactory<Row> funcFactory;

    /** */
    private final RowHandler.RowFactory<Row> rowFactory;

    /** */
    WindowPartitionBase(
        Comparator<Row> peerCmp,
        WindowFunctionFactory<Row> funcFactory,
        RowHandler.RowFactory<Row> rowFactory
    ) {
        this.peerCmp = peerCmp;
        this.funcFactory = funcFactory;
        this.rowFactory = rowFactory;
    }

    /** Creates {@link WindowFunctionWrapper} list. */
    final List<WindowFunctionWrapper<Row>> createWrappers() {
        return funcFactory.createWrappers();
    }

    /** Compares two rows and return true if current row peer not equal to the previous row peer. */
    protected final boolean isNewPeer(Row cur, @Nullable Row prev) {
        if (prev == null)
            return true;
        else if (peerCmp != null)
            return peerCmp.compare(prev, cur) != 0;
        else
            return false;
    }

    /** Creates row with window function results. */
    protected final Row createResultRow(RowHandler.RowFactory<Row> rowFactory, Row src, Object... results) {
        Row resultsRow = this.rowFactory.create(results);
        return rowFactory.handler().concat(src, resultsRow);
    }
}
