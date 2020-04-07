/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rex.RexCall;
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: Add class description.
 */
public class IndexScan implements Iterable<Object[]> {
    /** */
    private final ExecutionContext ectx;

    /** */
    private final TableDescriptor desc;

    /** */
    private final Predicate<Object[]> filters;

    /** */
    private final Function<Object[], Object[]> proj;

    /** */
    private final List<RexCall> indexConditions;

    /** */
    private final GridIndex idx;

    public IndexScan(
        ExecutionContext ctx,
        IgniteTable tbl,
        Predicate<Object[]> filters,
        Function<Object[], Object[]> proj,
        List<RexCall> indexConditions) {
        this.ectx = ctx;
        this.desc = tbl.descriptor();
        this.idx = tbl.index();
        this.filters = filters;
        this.proj = proj;
        this.indexConditions = indexConditions;


    }

    @NotNull @Override public Iterator<Object[]> iterator() {
        return null; // TODO: CODE: implement.
    }
}
