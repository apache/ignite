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

package org.apache.ignite.compute;

import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * This enumeration provides different types of actions following the last
 * received job result. See {@link ComputeTask#result(ComputeJobResult, List)} for
 * more details.
 */
public enum ComputeJobResultPolicy {
    /**
     * Wait for results if any are still expected. If all results have been received -
     * it will start reducing results.
     */
    WAIT,

    /** Ignore all not yet received results and start reducing results. */
    REDUCE,

    /**
     * Fail-over job to execute on another node.
     */
    FAILOVER;

    /** Enumerated values. */
    private static final ComputeJobResultPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static ComputeJobResultPolicy fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}