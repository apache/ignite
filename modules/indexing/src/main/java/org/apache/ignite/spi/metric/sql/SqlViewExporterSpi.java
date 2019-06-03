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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metrics.MetricNameUtils.MetricName;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.MetricRegistry;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metrics.MetricNameUtils.parse;

/**
 * This SPI implementation exports metrics as SQL views.
 */
public class SqlViewExporterSpi extends IgniteSpiAdapter implements MetricExporterSpi {
    /**
     * Metric registry.
     */
    private MetricRegistry reg;

    /**
     * Metric filter.
     */
    private @Nullable Predicate<Metric> filter;

    /**
     * Views waiting to be registered after indexing engine started.
     */
    private List<SqlSystemView> pendingViews;

    /**
     * Flag indicating that indexing engine was started.
     */
    private volatile boolean idxStarted;

    /**
     * Set of already registered as SQL view prefixes.
     */
    private Set<String> metricSets = new HashSet<>();

    /** */
    private Object regMux = new Object();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        reg.addMetricCreationListener(m -> {
            if (filter != null && !filter.test(m))
                return;

            MetricName n = parse(m.getName());

            if (metricSets.contains(n.msetName()))
                return;

            metricSets.add(n.msetName());

            registerSystemView(new MetricSetLocalSystemView(n.msetName(), reg, ((IgniteEx)ignite()).context()));
        });
    }

    /**
     * Registers new system view.
     * @param view View.
     */
    private void registerSystemView(SqlSystemView view) {
        synchronized (regMux) {
            if (!idxStarted) {
                if (pendingViews == null)
                    pendingViews = new ArrayList<>();

                pendingViews.add(view);

                return;
            }

            GridKernalContext ctx = ((IgniteEx)ignite()).context();

            SchemaManager mgr = ((IgniteH2Indexing)ctx.query().getIndexing()).schemaManager();

            mgr.createSysteView(QueryUtils.SCHEMA_MONITORING, view);


            if (log.isDebugEnabled())
                log.debug("MetricSet SQL view created. " + view.getTableName());
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        synchronized (regMux) {
            if (idxStarted)
                return;

            idxStarted = true;

            if (pendingViews != null) {
                for (SqlSystemView view : pendingViews)
                    registerSystemView(view);

                pendingViews = null;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void setMetricRegistry(MetricRegistry reg) {
        this.reg = reg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<Metric> filter) {
        this.filter = filter;
    }
}
