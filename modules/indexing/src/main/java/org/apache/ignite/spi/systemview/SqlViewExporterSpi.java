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

package org.apache.ignite.spi.systemview;

import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.QueryUtils.SCHEMA_SYS;

/**
 * This SPI implementation exports metrics as SQL views.
 *
 * Note, instance of this class created with reflection.
 * @see IgnitionEx#SYSTEM_VIEW_SQL_SPI
 */
public class SqlViewExporterSpi extends IgniteSpiAdapter implements SystemViewExporterSpi {
    /** System view filter. */
    @Nullable private Predicate<SystemView<?>> filter;

    /** System view registry. */
    private ReadOnlySystemViewRegistry sysViewReg;

    /** Schema manager. */
    private SchemaManager mgr;

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        GridKernalContext ctx = ((IgniteEx)ignite()).context();

        if (ctx.query().getIndexing() instanceof IgniteH2Indexing) {
            mgr = ((IgniteH2Indexing)ctx.query().getIndexing()).schemaManager();

            sysViewReg.forEach(this::register);
            sysViewReg.addSystemViewCreationListener(this::register);
        }
    }

    /**
     * Registers system view as SQL View.
     *
     * @param sysView System view.
     */
    private void register(SystemView<?> sysView) {
        if (filter != null && !filter.test(sysView)) {
            if (log.isDebugEnabled())
                log.debug("System view filtered and will not be registered [name=" + sysView.name() + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Found new system view [name=" + sysView.name() + ']');

        GridKernalContext ctx = ((IgniteEx)ignite()).context();

        SystemViewLocal<?> view = sysView instanceof FiltrableSystemView ?
            new FiltrableSystemViewLocal<>(ctx, sysView) : new SystemViewLocal<>(ctx, sysView);

        mgr.createSystemView(SCHEMA_SYS, view);
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
    @Override public void setSystemViewRegistry(ReadOnlySystemViewRegistry mlreg) {
        this.sysViewReg = mlreg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<SystemView<?>> filter) {
        this.filter = filter;
    }
}
