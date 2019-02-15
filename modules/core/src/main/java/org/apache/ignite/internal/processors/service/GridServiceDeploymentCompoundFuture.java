/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.jetbrains.annotations.Nullable;

/**
 * Service deployment compound future. If any exceptions are thrown during deployment, then {@link
 * IgniteCheckedException} with {@link ServiceDeploymentException} as a cause will be thrown from {@link
 * IgniteInternalFuture#get get()} method after all futures complete or fail. Inner exception will contain
 * configurations of failed services.
 */
public class GridServiceDeploymentCompoundFuture extends GridCompoundFuture<Object, Object> {
    /** Names of services written to cache during current deployment. */
    private Collection<String> svcsToRollback;

    /** */
    private volatile ServiceDeploymentException err;

    /** {@inheritDoc} */
    @Override protected boolean processFailure(Throwable err, IgniteInternalFuture<Object> fut) {
        assert fut instanceof GridServiceDeploymentFuture : fut;

        GridServiceDeploymentFuture depFut = (GridServiceDeploymentFuture)fut;

        synchronized (this) {
            if (this.err == null) {
                this.err = new ServiceDeploymentException("Failed to deploy some services.",
                    new ArrayList<ServiceConfiguration>());
            }

            this.err.getFailedConfigurations().add(depFut.configuration());
            this.err.addSuppressed(err);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Object res, @Nullable Throwable err, boolean cancel) {
        if (err == null && this.err != null)
            err = new IgniteCheckedException(this.err);

        return super.onDone(res, err, cancel);
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
}
