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

package org.apache.ignite.internal.processors.task;

/**
 * Defines keys for thread-local context in task processor.
 */
public enum GridTaskThreadContextKey {
    /** Task name. */
    TC_TASK_NAME,

    /** No failover flag. */
    TC_NO_FAILOVER,

    /** No result cache flag. */
    TC_NO_RESULT_CACHE,

    /** Projection for the task. */
    TC_SUBGRID,

    /** Projection predicate for the task. */
    TC_SUBGRID_PREDICATE,

    /** Timeout in milliseconds associated with the task. */
    TC_TIMEOUT,

    /** Security subject ID. */
    TC_SUBJ_ID,

    /** IO manager policy. */
    TC_IO_POLICY,

    /** Skip authorization for the task. */
    TC_SKIP_AUTH
}
