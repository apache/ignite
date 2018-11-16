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

package org.apache.ignite.services;

import org.jetbrains.annotations.Nullable;

/**
 * Service deployment failure policy describes rules how to handle errors occured during deployment.
 */
public enum ServiceDeploymentFailuresPolicy {
    /**
     * When service deployment failures policy is {@code CANCEL}, then deployed service's instances will be canceled and
     * exception propagated to all nodes in case of any errors on deployment/reassignment.
     */
    CANCEL,

    /**
     * When service deployment failures policy is {@code IGNORE}, then any errors on deployment/reassignment will be
     * ignored and services deployed as is, also exception will be propagated to all nodes.
     */
    IGNORE;

    /** Enumerated values. */
    private static final ServiceDeploymentFailuresPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static ServiceDeploymentFailuresPolicy fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
