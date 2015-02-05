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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.hadoop.jobtracker.*;
import org.apache.ignite.internal.processors.hadoop.planner.*;
import org.apache.ignite.internal.processors.hadoop.shuffle.*;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.*;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.hadoop.GridHadoopClassLoader.*;

/**
 * Hadoop processor.
 */
public class IgniteHadoopProcessor extends IgniteHadoopProcessorAdapter {
    /** Job ID counter. */
    private final AtomicInteger idCtr = new AtomicInteger();

    /** Hadoop context. */
    @GridToStringExclude
    private GridHadoopContext hctx;

    /** Hadoop facade for public API. */
    @GridToStringExclude
    private GridHadoop hadoop;

    /**
     * @param ctx Kernal context.
     */
    public IgniteHadoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        GridHadoopConfiguration cfg = ctx.config().getHadoopConfiguration();

        if (cfg == null)
            cfg = new GridHadoopConfiguration();
        else
            cfg = new GridHadoopConfiguration(cfg);

        initializeDefaults(cfg);

        validate(cfg);

        if (hadoopHome() != null)
            U.quietAndInfo(log, "HADOOP_HOME is set to " + hadoopHome());

        boolean ok = false;

        try { // Check for Hadoop installation.
            hadoopUrls();

            ok = true;
        }
        catch (IgniteCheckedException e) {
            U.quietAndWarn(log, e.getMessage());
        }

        if (ok) {
            hctx = new GridHadoopContext(
                ctx,
                cfg,
                new GridHadoopJobTracker(),
                cfg.isExternalExecution() ? new GridHadoopExternalTaskExecutor() : new GridHadoopEmbeddedTaskExecutor(),
                new GridHadoopShuffle());


            for (GridHadoopComponent c : hctx.components())
                c.start(hctx);

            hadoop = new GridHadoopImpl(this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteHadoopProcessor.class, this);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if (hctx == null)
            return;

        List<GridHadoopComponent> components = hctx.components();

        for (ListIterator<GridHadoopComponent> it = components.listIterator(components.size()); it.hasPrevious();) {
            GridHadoopComponent c = it.previous();

            c.stop(cancel);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        if (hctx == null)
            return;

        for (GridHadoopComponent c : hctx.components())
            c.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (hctx == null)
            return;

        List<GridHadoopComponent> components = hctx.components();

        for (ListIterator<GridHadoopComponent> it = components.listIterator(components.size()); it.hasPrevious();) {
            GridHadoopComponent c = it.previous();

            c.onKernalStop(cancel);
        }
    }

    /**
     * Gets Hadoop context.
     *
     * @return Hadoop context.
     */
    public GridHadoopContext context() {
        return hctx;
    }

    /** {@inheritDoc} */
    @Override public GridHadoop hadoop() {
        if (hadoop == null)
            throw new IllegalStateException("Hadoop accelerator is disabled (Hadoop is not in classpath, " +
                "is HADOOP_HOME environment variable set?)");

        return hadoop;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration config() {
        return hctx.configuration();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId nextJobId() {
        return new GridHadoopJobId(ctx.localNodeId(), idCtr.incrementAndGet());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        return hctx.jobTracker().submit(jobId, jobInfo);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus status(GridHadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().status(jobId);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounters counters(GridHadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().jobCounters(jobId);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> finishFuture(GridHadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().finishFuture(jobId);
    }

    /** {@inheritDoc} */
    @Override public boolean kill(GridHadoopJobId jobId) throws IgniteCheckedException {
        return hctx.jobTracker().killJob(jobId);
    }

    /**
     * Initializes default hadoop configuration.
     *
     * @param cfg Hadoop configuration.
     */
    private void initializeDefaults(GridHadoopConfiguration cfg) {
        if (cfg.getMapReducePlanner() == null)
            cfg.setMapReducePlanner(new GridHadoopDefaultMapReducePlanner());
    }

    /**
     * Validates Grid and Hadoop configuration for correctness.
     *
     * @param hadoopCfg Hadoop configuration.
     * @throws IgniteCheckedException If failed.
     */
    private void validate(GridHadoopConfiguration hadoopCfg) throws IgniteCheckedException {
        if (ctx.config().isPeerClassLoadingEnabled())
            throw new IgniteCheckedException("Peer class loading cannot be used with Hadoop (disable it using " +
                "GridConfiguration.setPeerClassLoadingEnabled()).");
    }
}
