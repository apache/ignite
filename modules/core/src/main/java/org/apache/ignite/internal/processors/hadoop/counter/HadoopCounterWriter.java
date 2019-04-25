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

package org.apache.ignite.internal.processors.hadoop.counter;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;

/**
 * The object that writes some system counters to some storage for each running job. This operation is a part of
 * whole statistics collection process.
 */
public interface HadoopCounterWriter {
    /**
     * Writes counters of given job to some statistics storage.
     *
     * @param job The job.
     * @param cntrs Counters.
     * @throws IgniteCheckedException If failed.
     */
    public void write(HadoopJobEx job, HadoopCounters cntrs) throws IgniteCheckedException;
}