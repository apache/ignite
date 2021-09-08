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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.processors.cache.AbstractCachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.security.SecurityContext;

/**
 *
 */
public class RebalanceReassignExchangeTask extends AbstractCachePartitionExchangeWorkerTask {
    /** */
    private final GridDhtPartitionExchangeId exchId;

    /** */
    private final GridDhtPartitionsExchangeFuture exchFut;

    /**
     * @param secCtx Security context in which current task must be executed.
     * @param exchId Exchange ID.
     * @param exchFut Exchange future.
     */
    public RebalanceReassignExchangeTask(
        SecurityContext secCtx,
        GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchFut
    ) {
        super(secCtx);

        assert exchId != null;
        assert exchFut != null;

        this.exchId = exchId;
        this.exchFut = exchFut;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Exchange future.
     */
    public GridDhtPartitionsExchangeFuture future() {
        return exchFut;
    }
}
