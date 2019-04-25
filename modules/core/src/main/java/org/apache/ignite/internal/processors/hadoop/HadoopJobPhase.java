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

package org.apache.ignite.internal.processors.hadoop;

/**
 * Job run phase.
 */
public enum HadoopJobPhase {
    /** Job is running setup task. */
    PHASE_SETUP,

    /** Job is running map and combine tasks. */
    PHASE_MAP,

    /** Job has finished all map tasks and running reduce tasks. */
    PHASE_REDUCE,

    /** Job is stopping due to exception during any of the phases. */
    PHASE_CANCELLING,

    /** Job has finished execution. */
    PHASE_COMPLETE
}