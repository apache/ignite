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

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.future.*;

/**
 * Hadoop processor.
 */
public class GridHadoopNoopProcessor extends GridHadoopProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public GridHadoopNoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public GridHadoop hadoop() {
        throw new IllegalStateException("Hadoop module is not found in class path.");
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration config() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId nextJobId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        return new GridFinishedFutureEx<>(new IgniteCheckedException("Hadoop is not available."));
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus status(GridHadoopJobId jobId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounters counters(GridHadoopJobId jobId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> finishFuture(GridHadoopJobId jobId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean kill(GridHadoopJobId jobId) throws IgniteCheckedException {
        return false;
    }
}
