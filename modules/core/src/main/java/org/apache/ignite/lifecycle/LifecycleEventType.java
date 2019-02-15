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

package org.apache.ignite.lifecycle;

import org.jetbrains.annotations.Nullable;

/**
 * Node lifecycle event types. These events are used to notify lifecycle beans
 * about changes in node lifecycle state.
 * <p>
 * For more information and detailed examples refer to {@link org.apache.ignite.lifecycle.LifecycleBean}
 * documentation.
 */
public enum LifecycleEventType {
    /**
     * Invoked before node startup routine. Node is not
     * initialized and cannot be used.
     */
    BEFORE_NODE_START,

    /**
     * Invoked after node startup is complete. Node is fully
     * initialized and fully functional.
     */
    AFTER_NODE_START,

    /**
     * Invoked before node stopping routine. Node is fully functional
     * at this point.
     */
    BEFORE_NODE_STOP,

    /**
     * Invoked after node had stopped. Node is stopped and
     * cannot be used.
     */
    AFTER_NODE_STOP;

    /** Enumerated values. */
    private static final LifecycleEventType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static LifecycleEventType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}