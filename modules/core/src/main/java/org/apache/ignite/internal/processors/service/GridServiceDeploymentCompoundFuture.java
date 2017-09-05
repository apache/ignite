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

package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.jetbrains.annotations.Nullable;

/**
 * Service deployment compound future, {@code allOrNone} parameter specifies failing policy.
 * <p>
 * If {@code allOrNone} parameter is set to {@code false}, then this future waits for completion of all child futures.
 * If any exceptions are thrown during deployment, then {@link IgniteCheckedException} with {@link
 * ServiceDeploymentException} as a cause will be thrown from {@link IgniteInternalFuture#get get()} method after all
 * futures complete or fail. Inner exception will contain configurations of failed services.
 */
public class GridServiceDeploymentCompoundFuture extends GridCompoundFuture<Object, Object> {
    /** */
    private final boolean allOrNone;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Names of services written to cache during current deployment. */
    private Collection<String> svcsToRollback;

    /** */
    private volatile ServiceDeploymentException err;

    /**
     * @param allOrNone Failing policy.
     * @param ctx Kernal context.
     */
    GridServiceDeploymentCompoundFuture(boolean allOrNone, GridKernalContext ctx) {
        this.allOrNone = allOrNone;
        this.ctx = ctx;
        this.log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override protected boolean processFailure(Throwable err, IgniteInternalFuture<Object> fut) {
        assert fut instanceof GridServiceDeploymentFuture : fut;

        GridServiceDeploymentFuture depFut = (GridServiceDeploymentFuture)fut;

        if (allOrNone) {
            if (initialized()) {
                onDone(new IgniteCheckedException(
                    new ServiceDeploymentException("Failed to deploy provided services.", err, getConfigurations())));
            }
            else {
                synchronized (this) {
                    if (this.err == null) {
                        this.err = new ServiceDeploymentException("Failed to deploy provided services.", err,
                            new ArrayList<ServiceConfiguration>());
                    }
                    else
                        this.err.addSuppressed(err);
                }
            }
        }
        else {
            synchronized (this) {
                if (this.err == null)
                    this.err = new ServiceDeploymentException("Failed to deploy some services.",
                        new ArrayList<ServiceConfiguration>());

                this.err.getFailedConfigurations().add(depFut.configuration());
                this.err.addSuppressed(err);
            }
        }

        return true;
    }

    /**
     * Marks this future as initialized. Will complete with error if failures before initialization occurred and
     * all-or-none policy is followed.
     */
    public void serviceDeploymentMarkInitialized() {
        if (allOrNone && this.err != null) {
            this.err.getFailedConfigurations().addAll(getConfigurations());

            onDone(new IgniteCheckedException(this.err));
        }
        else
            super.markInitialized();
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable final Object res, @Nullable Throwable err, final boolean cancel) {
        final Throwable resErr;

        if (err == null && this.err != null)
            resErr = new IgniteCheckedException(this.err);
        else
            resErr = err;

        if (allOrNone && this.err != null && svcsToRollback != null) {
            U.warn(log, "Failed to deploy provided services. The following services will be cancelled:" + svcsToRollback);

            IgniteInternalFuture<?> fut = ctx.service().cancelAll(svcsToRollback);

            /*
            Can not call fut.get() since it is possible we are in system pool now and
            fut also should be completed from system pool.
             */
            fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    try {
                        fut.get();
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to cancel deployed services.", e);
                    }
                    finally {
                        svcsToRollback = null;
                    }

                    GridServiceDeploymentCompoundFuture.super.onDone(res, resErr, cancel);
                }
            });

            return false;
        }

        return super.onDone(res, resErr, cancel);
    }

    /**
     * @param fut Child future.
     * @param own If {@code true}, then corresponding service will be cancelled on failure.
     */
    public void add(GridServiceDeploymentFuture fut, boolean own) {
        super.add(fut);

        if (own) {
            if (svcsToRollback == null)
                svcsToRollback = new ArrayList<>();

            svcsToRollback.add(fut.configuration().getName());
        }
    }

    /**
     * @return Collection of names of services that were written to cache during current deployment.
     */
    public Collection<String> servicesToRollback() {
        if (svcsToRollback != null)
            return svcsToRollback;
        else
            return Collections.emptyList();
    }

    /**
     * @return Collection of configurations, stored in child futures.
     */
    private Collection<ServiceConfiguration> getConfigurations() {
        Collection<IgniteInternalFuture<Object>> futs = futures();

        List<ServiceConfiguration> cfgs = new ArrayList<>(futs.size());

        for (IgniteInternalFuture<Object> fut : futs)
            cfgs.add(((GridServiceDeploymentFuture)fut).configuration());

        return cfgs;
    }
}
