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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.hadoop.mapreduce.IgniteHadoopMapReducePlanner;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobTracker;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffle;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopEmbeddedTaskExecutor;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hadoop processor.
 */
public class HadoopProcessor extends HadoopProcessorAdapter {
    /** Job ID counter. */
    private final AtomicInteger idCtr = new AtomicInteger();

    /** Hadoop context. */
    @GridToStringExclude
    private HadoopContext hctx;

    /** Hadoop facade for public API. */
    @GridToStringExclude
    private Hadoop hadoop;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public HadoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        HadoopConfiguration cfg = ctx.config().getHadoopConfiguration();

        if (cfg == null)
            cfg = new HadoopConfiguration();
        else
            cfg = new HadoopConfiguration(cfg);

        initializeDefaults(cfg);

        hctx = new HadoopContext(
            ctx,
            cfg,
            new HadoopJobTracker(),
            new HadoopEmbeddedTaskExecutor(),
            // TODO: IGNITE-404: Uncomment when fixed.
            //cfg.isExternalExecution() ? new HadoopExternalTaskExecutor() : new HadoopEmbeddedTaskExecutor(),
            new HadoopShuffle());

        for (HadoopComponent c : hctx.components())
            c.start(hctx);

        hadoop = new HadoopImpl(this);

        ctx.addNodeAttribute(HadoopAttributes.NAME, new HadoopAttributes(cfg));
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        if (hctx == null)
            return;

        for (HadoopComponent c : hctx.components())
            c.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (hctx == null)
            return;

        List<HadoopComponent> components = hctx.components();

        for (ListIterator<HadoopComponent> it = components.listIterator(components.size()); it.hasPrevious();) {
            HadoopComponent c = it.previous();

            c.onKernalStop(cancel);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if (hctx == null)
            return;

        List<HadoopComponent> components = hctx.components();

        for (ListIterator<HadoopComponent> it = components.listIterator(components.size()); it.hasPrevious();) {
            HadoopComponent c = it.previous();

            c.stop(cancel);
        }
    }

    /**
     * Gets Hadoop context.
     *
     * @return Hadoop context.
     */
    public HadoopContext context() {
        return hctx;
    }

    /** {@inheritDoc} */
    @Override public Hadoop hadoop() {
        if (hadoop == null)
            throw new IllegalStateException("Hadoop accelerator is disabled (Hadoop is not in classpath, " +
                "is HADOOP_HOME environment variable set?)");

        return hadoop;
    }

    /** {@inheritDoc} */
    @Override public HadoopConfiguration config() {
        return hctx.configuration();
    }

    /** {@inheritDoc} */
    @Override public HadoopJobId nextJobId() {
        return new HadoopJobId(ctx.localNodeId(), idCtr.incrementAndGet());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> submit(HadoopJobId jobId, HadoopJobInfo jobInfo) {
        return hctx.jobTracker().submit(jobId, jobInfo);
    }

    /** {@inheritDoc} */
    @Override public HadoopJobStatus status(HadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().status(jobId);
    }

    /** {@inheritDoc} */
    @Override public HadoopCounters counters(HadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().jobCounters(jobId);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> finishFuture(HadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().finishFuture(jobId);
    }

    /** {@inheritDoc} */
    @Override public boolean kill(HadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().killJob(jobId);
    }

    /** {@inheritDoc} */
    @Override public void validateEnvironment() throws IgniteCheckedException {
        // Perform some static checks as early as possible, so that any recoverable exceptions are thrown here.
        try {
            HadoopLocations loc = HadoopClasspathUtils.locations();

            if (!F.isEmpty(loc.home()))
                U.quietAndInfo(log, HadoopClasspathUtils.HOME + " is set to " + loc.home());

            U.quietAndInfo(log, "Resolved Hadoop classpath locations: " + loc.common() + ", " + loc.hdfs() + ", " +
                loc.mapred());
        }
        catch (IOException ioe) {
            throw new IgniteCheckedException(ioe.getMessage(), ioe);
        }

        HadoopClassLoader.hadoopUrls();
    }

    /**
     * Initializes default hadoop configuration.
     *
     * @param cfg Hadoop configuration.
     */
    private void initializeDefaults(HadoopConfiguration cfg) {
        if (cfg.getMapReducePlanner() == null)
            cfg.setMapReducePlanner(new IgniteHadoopMapReducePlanner());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopProcessor.class, this);
    }
}