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