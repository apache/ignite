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

package org.apache.ignite.mxbean;

import java.util.List;

/**
 * MBean that provides ability to terminate worker that registered in the workers registry.
 */
@MXBeanDescription("MBean that provides ability to terminate worker that registered in the workers registry.")
public interface WorkersControlMXBean {
    /**
     * Returns names of all registered workers.
     *
     * @return Worker names.
     */
    @MXBeanDescription("Names of registered workers.")
    public List<String> getWorkerNames();

    /**
     * Terminates worker.
     *
     * @param name Worker name.
     * @return {@code True} if worker has been terminated successfully, {@code false} otherwise.
     */
    @MXBeanDescription("Terminates worker.")
    @MXBeanParametersNames(
        "name"
    )
    @MXBeanParametersDescriptions(
        "Name of worker to terminate."
    )
    public boolean terminateWorker(String name);

    /**
     * Stops thread by {@code name}, if exists and unique.
     *
     * @param name Thread name.
     * @return {@code True} if thread has been stopped successfully, {@code false} otherwise.
     */
    @MXBeanDescription("Stops thread by unique name.")
    @MXBeanParametersNames(
        "name"
    )
    @MXBeanParametersDescriptions(
        "Name of thread to stop."
    )
    public boolean stopThreadByUniqueName(String name);

    /**
     * Stops thread by {@code id}, if exists.
     *
     * @param id Thread id.
     * @return {@code True} if thread has been stopped successfully, {@code false} otherwise.
     */
    @MXBeanDescription("Stops thread by id.")
    @MXBeanParametersNames(
        "id"
    )
    @MXBeanParametersDescriptions(
        "Id of thread to stop."
    )
    public boolean stopThreadById(long id);
}
