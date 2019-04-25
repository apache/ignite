/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.jetbrains.annotations.Nullable;

/**
 * Possible states of {@link org.apache.ignite.Ignition}. You can register a listener for
 * state change notifications via {@link org.apache.ignite.Ignition#addListener(IgnitionListener)}
 * method.
 */
public enum IgniteState {
    /**
     * Grid factory started.
     */
    STARTED,

    /**
     * Grid factory stopped.
     */
    STOPPED,

    /**
     * Grid factory stopped due to network segmentation issues.
     * <p>
     * Notification on this state will be fired only when segmentation policy is
     * set to {@link SegmentationPolicy#STOP} or {@link SegmentationPolicy#RESTART_JVM}
     * and node is stopped from internals of Ignite after segment becomes invalid.
     */
    STOPPED_ON_SEGMENTATION,

    /**
     * Grid factory stopped due to a critical failure.
     */
    STOPPED_ON_FAILURE;

    /** Enumerated values. */
    private static final IgniteState[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static IgniteState fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}