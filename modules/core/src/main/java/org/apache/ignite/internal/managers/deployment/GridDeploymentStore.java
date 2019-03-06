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

package org.apache.ignite.internal.managers.deployment;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for all deployment stores.
 */
public interface GridDeploymentStore {
    /**
     * Starts store.
     *
     * @throws IgniteCheckedException If start failed.
     */
    public void start() throws IgniteCheckedException;

    /**
     * Stops store.
     */
    public void stop();

    /**
     * Kernal started callback.
     *
     * @throws IgniteCheckedException If callback execution failed.
     */
    public void onKernalStart() throws IgniteCheckedException;

    /**
     * Kernel stopping callback.
     */
    public void onKernalStop();

    /**
     * @param meta Deployment metadata.
     * @return Deployment.
     */
    @Nullable public GridDeployment getDeployment(GridDeploymentMetadata meta);

    /**
     * Gets class loader based on ID.
     *
     *
     * @param ldrId Class loader ID.
     * @return Class loader of {@code null} if not found.
     */
    @Nullable public GridDeployment getDeployment(IgniteUuid ldrId);

    /**
     * @return All current deployments.
     */
    public Collection<GridDeployment> getDeployments();

    /**
     * Explicitly deploys class.
     *
     * @param cls Class to explicitly deploy.
     * @param clsLdr Class loader.
     * @return Grid deployment.
     * @throws IgniteCheckedException Id deployment failed.
     */
    public GridDeployment explicitDeploy(Class<?> cls, ClassLoader clsLdr) throws IgniteCheckedException;

    /**
     * @param nodeId Optional ID of node that initiated request.
     * @param rsrcName Undeploys all deployments that have given
     */
    public void explicitUndeploy(@Nullable UUID nodeId, String rsrcName);

    /**
     * Adds participants to all deployments.
     *
     * @param allParticipants All participants to determine which deployments to add to.
     * @param addedParticipants Participants to add.
     */
    public void addParticipants(Map<UUID, IgniteUuid> allParticipants,
        Map<UUID, IgniteUuid> addedParticipants);
}