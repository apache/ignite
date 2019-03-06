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

package org.apache.ignite.internal;

import org.jetbrains.annotations.Nullable;

/**
 * Distribution modes for one or more closures executed via {@code Grid.call(...)} methods.
 * In other words, given a set of jobs (closures) and set of grid nodes this enumeration provides
 * three simple modes of how these jobs will be mapped to the nodes.
 * <p>
 * <b>Note</b> that if you need to provide custom distribution logic you need to
 * implement {@link org.apache.ignite.compute.ComputeTask} interface that allows you to customize every aspect of a
 * distributed Java code execution such as  mapping, load balancing, failover, collision
 * resolution, continuations, etc.
 */
public enum GridClosureCallMode {
    /**
     * In this mode all closures will be executed on each node.
     * This mode essentially allows to <tt>broadcast</tt> executions to all nodes.
     */
    BROADCAST,

    /**
     * In this mode closures will be executed on the nodes provided by the default
     * load balancer.
     * <p>
     * NOTE: this mode <b>must</b> be used for all cache affinity routing. Load balance
     * manager has specific logic to handle co-location between compute grid jobs and
     * in-memory data grid data. All other modes will not work for affinity-based co-location.
     */
    BALANCE;

    /** Enumerated values. */
    private static final GridClosureCallMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridClosureCallMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}