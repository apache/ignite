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

package org.apache.ignite.testframework.junits;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.GridKernalGatewayImpl;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test context.
 */
public class GridTestKernalContext extends GridKernalContextImpl {
    /**
     * @param log Logger to use in context config.
     */
    public GridTestKernalContext(IgniteLogger log) {
        this(log, new IgniteConfiguration());

        try {
            add(new IgnitePluginProcessor(this, config(), Collections.<PluginProvider>emptyList()));
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException("Must not fail for empty plugins list.", e);
        }
    }

    /**
     * @param log Logger to use in context config.
     * @param cfg Configuration to use in Test
     */
    public GridTestKernalContext(IgniteLogger log, IgniteConfiguration cfg) {
        super(new GridLoggerProxy(log, null, null, null),
                new IgniteKernal(null),
                cfg,
                new GridKernalGatewayImpl(null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                cfg.getPluginProviders() != null && cfg.getPluginProviders().length > 0 ?
                    Arrays.asList(cfg.getPluginProviders()) : U.allPluginProviders(),
                null,
                null,
                null,
                new LongJVMPauseDetector(log)
        );

        GridTestUtils.setFieldValue(grid(), "cfg", config());
        GridTestUtils.setFieldValue(grid(), "ctx", this);

        config().setGridLogger(log);

        if (cfg.getMetricExporterSpi() == null || cfg.getMetricExporterSpi().length == 0)
            cfg.setMetricExporterSpi(new NoopMetricExporterSpi());

        add(new GridMetricManager(this));
        add(new GridResourceProcessor(this));
        add(new GridInternalSubscriptionProcessor(this));
    }

    /**
     * Starts everything added (in the added order).
     *
     * @throws IgniteCheckedException If failed
     */
    public void start() throws IgniteCheckedException {
        for (GridComponent comp : this)
            comp.start();
    }

    /**
     * Stops everything added.
     *
     * @param cancel Cancel parameter.
     * @throws IgniteCheckedException If failed.
     */
    public void stop(boolean cancel) throws IgniteCheckedException {
        List<GridComponent> comps = components();

        for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
            GridComponent comp = it.previous();

            comp.stop(cancel);
        }
    }

    /**
     * Sets system executor service.
     *
     * @param sysExecSvc Executor service
     */
    public void setSystemExecutorService(ExecutorService sysExecSvc) {
        this.sysExecSvc = sysExecSvc;
    }

    /**
     * Sets executor service.
     *
     * @param execSvc Executor service
     */
    public void setExecutorService(ExecutorService execSvc){
        this.execSvc = execSvc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTestKernalContext.class, this, super.toString());
    }
}
