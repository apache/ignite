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

package org.apache.ignite.internal.processors.cluster;

/**
 * Used only for logging purposes.
 *
 * Enables to show in logs what was the cause of the last change
 * of {@link BaselineTopology#branchingPntHash branching point hash}.
 */
enum BranchingPointType {
    /** */
    CLUSTER_ACTIVATION("Cluster activation"),

    /** */
    NEW_BASELINE_TOPOLOGY("New BaselineTopology"),

    /** */
    BRANCHING_HISTORY_RESET("Branching history reset");

    /** */
    private String type;

    /** */
    BranchingPointType(String type) {
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return type;
    }
}
