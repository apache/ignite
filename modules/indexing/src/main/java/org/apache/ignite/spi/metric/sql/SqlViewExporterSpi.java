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

import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.metric.view.SystemView;
import org.jetbrains.annotations.Nullable;

/**
 * This SPI implementation exports metrics as SQL views.
 */
public class SqlViewExporterSpi extends IgniteSpiAdapter implements MetricExporterSpi {
    /** System view name. */
    public static final String SYS_VIEW_NAME = "METRICS";

    /** Metric filter. */
    private @Nullable Predicate<MetricRegistry> mregFilter;

    /** System view filter. */
    private @Nullable Predicate<SystemView<?>> sviewFilter;

    /** Metric Registry. */
    private ReadOnlyMetricRegistry mreg;

    /** System view registry. */
    private ReadOnlySystemViewRegistry svreg;

    /** Schema manager. */
    private SchemaManager mgr;

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        GridKernalContext ctx = ((IgniteEx)ignite()).context();

        this.mgr = ((IgniteH2Indexing)ctx.query().getIndexing()).schemaManager();

        mgr.createSystemView(new MetricRegistryLocalSystemView(ctx, mreg, mregFilter));

        if (log.isDebugEnabled())
            log.debug(SYS_VIEW_NAME + " SQL view for metrics created.");

        svreg.forEach(this::register);
        svreg.addSystemViewCreationListener(this::register);
    }

    /**
     * Registers system view as SQL View.
     *
     * @param sview System view.
     */
    private void register(SystemView<?> sview) {
        if (sviewFilter != null && !sviewFilter.test(sview)) {
            if (log.isDebugEnabled())
                U.debug(log, "System view filtered and will not be registered.[name=" + sview.name() + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Found new system view [name=" + sview.name() + ']');

        GridKernalContext ctx = ((IgniteEx)ignite()).context();

        SystemViewLocalSystemView<?> view = new SystemViewLocalSystemView<>(ctx, sview);

        mgr.createSystemView(view);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setMetricRegistry(ReadOnlyMetricRegistry mreg) {
        this.mreg = mreg;
    }

    /** {@inheritDoc} */
    @Override public void setSystemViewRegistry(ReadOnlySystemViewRegistry mlreg) {
        this.svreg = mlreg;
    }

    /** {@inheritDoc} */
    @Override public void setMetricExportFilter(Predicate<MetricRegistry> filter) {
        this.mregFilter = filter;
    }

    /** {@inheritDoc} */
    @Override public void setSystemViewExportFilter(Predicate<SystemView<?>> filter) {
        this.sviewFilter = filter;
    }
}
