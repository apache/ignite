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

package org.apache.ignite.spi.metric.sql;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Sql view for exporting metrics.
 */
public class MetricRegistryLocalSystemView extends SqlAbstractLocalSystemView {
    /** Metric registry. */
    private ReadOnlyMetricManager mreg;

    /** Metric filter. */
    private @Nullable Predicate<ReadOnlyMetricRegistry> filter;

    /**
     * @param ctx Context.
     * @param mreg Metric registry.
     * @param filter Metric registry filter.
     */
    public MetricRegistryLocalSystemView(GridKernalContext ctx, ReadOnlyMetricManager mreg,
        @Nullable Predicate<ReadOnlyMetricRegistry> filter) {
        super(SqlViewMetricExporterSpi.SYS_VIEW_NAME, "Ignite metrics",
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
            private Iterator<ReadOnlyMetricRegistry> grps = mreg.iterator();

            /** */
            private Iterator<Metric> curr = Collections.emptyIterator();

            /** */
            private boolean advance() {
                while (grps.hasNext()) {
                    ReadOnlyMetricRegistry mreg = grps.next();

                    if (filter != null && !filter.test(mreg))
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
