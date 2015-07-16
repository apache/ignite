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

package org.apache.ignite.internal;

import org.jetbrains.annotations.*;

/**
 * Kernal life cycle states.
 */
public enum GridKernalState {
    /** Kernal is started. */
    STARTED,

    /** Kernal is starting.*/
    STARTING,

    /** Kernal is stopping. */
    STOPPING,

    /** Kernal is disconnected. */
    DISCONNECTED,

    /** Kernal is stopped.
     * <p>
     * This is also the initial state of the kernal.
     */
    STOPPED;

    /** Enum values. */
    private static final GridKernalState[] VALS = values();

    /**
     * @param ord Byte to convert to enum.
     * @return Enum.
     */
    @Nullable public static GridKernalState fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
