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
package org.apache.ignite.internal.processors.affinity;

/**
 * Interface for historical calculated affinity assignment.
 */
public interface HistoryAffinityAssignment extends AffinityAssignment {
    /**
     * Should return true if instance is "heavy" and should be taken into account during history size management.
     *
     * @return <code>true</code> if adding this instance to history should trigger size check and possible cleanup.
     */
    public boolean requiresHistoryCleanup();

    /**
     * In case this instance is lightweight wrapper of another instance, this method should return reference
     * to an original one. Otherwise, it should return <code>this</code> reference.
     *
     * @return Original instance of <code>this</code> if not applicable.
     */
    public HistoryAffinityAssignment origin();
}
