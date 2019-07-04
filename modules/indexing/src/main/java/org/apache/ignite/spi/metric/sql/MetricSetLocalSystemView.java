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

package org.apache.ignite.spi.metric.sql;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Sql view for exporting metrics.
 */
public class MetricSetLocalSystemView extends SqlAbstractLocalSystemView {
    /** Metric registry. */
    private ReadOnlyMetricRegistry mreg;

    /** Metric filter. */
    private @Nullable Predicate<MetricRegistry> filter;

    /**
     * @param ctx Context.
     * @param mreg Metric registry.
     * @param filter Metric registry filter.
     */
    public MetricSetLocalSystemView(GridKernalContext ctx, ReadOnlyMetricRegistry mreg,
        @Nullable Predicate<MetricRegistry> filter) {
        super(SqlViewExporterSpi.SYS_VIEW_NAME, "Ignite metrics",
            ctx,
            newColumn("NAME", Value.STRING),
            newColumn("VALUE", Value.STRING),
            newColumn("DESCRIPTION", Value.STRING));

        this.mreg = mreg;
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        return new Iterator<Row>() {
            /** */
            private Iterator<MetricRegistry> grps = mreg.iterator();

            /** */
            private Iterator<Metric> curr = Collections.emptyIterator();

            /** */
            private boolean advance() {
                while (grps.hasNext()) {
                    MetricRegistry mreg = grps.next();

                    if (!filter.test(mreg))
                        continue;

                    curr = mreg.iterator();

                    if (curr.hasNext())
                        return true;
                }

                return false;
            }

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                if (curr.hasNext())
                    return true;

                return advance();
            }

            /** {@inheritDoc} */
            @Override public Row next() {
                Metric m = curr.next();

                return createRow(ses, m.name(), m.getAsString(), m.description());
            }
        };
    }
}
