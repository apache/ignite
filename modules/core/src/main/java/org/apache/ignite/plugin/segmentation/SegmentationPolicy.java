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

package org.apache.ignite.plugin.segmentation;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Policy that defines how node will react on topology segmentation. Note that default
 * segmentation policy is defined by {@link IgniteConfiguration#DFLT_SEG_PLC} property.
 * @see SegmentationResolver
 */
public enum SegmentationPolicy {
    /**
     * When segmentation policy is {@code RESTART_JVM}, all listeners will receive
     * {@link org.apache.ignite.events.EventType#EVT_NODE_SEGMENTED} event and then JVM will be restarted.
     * Note, that this will work <b>only</b> if Ignite is started with {@link org.apache.ignite.startup.cmdline.CommandLineStartup}
     * via standard {@code ignite.{sh|bat}} shell script.
     */
    RESTART_JVM,

    /**
     * When segmentation policy is {@code STOP}, all listeners will receive
     * {@link org.apache.ignite.events.EventType#EVT_NODE_SEGMENTED} event and then particular grid node
     * will be stopped via call to {@link org.apache.ignite.Ignition#stop(String, boolean)}.
     */
    STOP,

    /**
     * When segmentation policy is {@code NOOP}, all listeners will receive
     * {@link org.apache.ignite.events.EventType#EVT_NODE_SEGMENTED} event and it is up to user to
     * implement logic to handle this event.
     */
    NOOP;

    /** Enumerated values. */
    private static final SegmentationPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static SegmentationPolicy fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
