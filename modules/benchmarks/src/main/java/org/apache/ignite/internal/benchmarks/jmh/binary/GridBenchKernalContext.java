package org.apache.ignite.internal.benchmarks.jmh.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.GridKernalGatewayImpl;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;


/**
 * Test context.
 */
public class GridBenchKernalContext extends GridKernalContextImpl {
    /**
     * @param log Logger to use in context config.
     */
    public GridBenchKernalContext(IgniteLogger log) {
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
    public GridBenchKernalContext(IgniteLogger log, IgniteConfiguration cfg) {
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
                cfg.getPluginProviders() != null && cfg.getPluginProviders().length > 0 ?
                        Arrays.asList(cfg.getPluginProviders()) : U.allPluginProviders(),
                null,
                null,
                null,
                new LongJVMPauseDetector(log)
        );

        IgniteEx igniteEx = grid();

        try {
            IgniteUtils.findField(igniteEx.getClass(), "cfg").set(igniteEx, config());
            IgniteUtils.findField(igniteEx.getClass(), "ctx").set(igniteEx, this);
        } catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }

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
    public void setExecutorService(ExecutorService execSvc) {
        this.execSvc = execSvc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridBenchKernalContext.class, this, super.toString());
    }
}
