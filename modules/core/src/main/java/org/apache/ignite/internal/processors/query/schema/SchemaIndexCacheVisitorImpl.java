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

package org.apache.ignite.internal.processors.query.schema;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;

/**
 * Visitor who create/rebuild indexes in parallel by partition for a given cache.
 */
public class SchemaIndexCacheVisitorImpl implements SchemaIndexCacheVisitor {
    /** Cache context. */
    private final GridCacheContext cctx;

    /** Row filter. */
    private final SchemaIndexCacheFilter rowFilter;

    /** Cancellation token. */
    private final SchemaIndexOperationCancellationToken cancel;

    /** Future for create/rebuild index. */
    protected final GridFutureAdapter<Void> buildIdxFut;

    /** Logger. */
    protected IgniteLogger log;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param rowFilter Row filter.
     * @param cancel Cancellation token.
     * @param buildIdxFut Future for create/rebuild index.
     */
    public SchemaIndexCacheVisitorImpl(
        GridCacheContext cctx,
        @Nullable SchemaIndexCacheFilter rowFilter,
        @Nullable SchemaIndexOperationCancellationToken cancel,
        GridFutureAdapter<Void> buildIdxFut
    ) {
        assert nonNull(cctx);
        assert nonNull(buildIdxFut);

        if (cctx.isNear())
            cctx = ((GridNearCacheAdapter)cctx.cache()).dht().context();

        this.cctx = cctx;
        this.buildIdxFut = buildIdxFut;

        this.cancel = cancel;
        this.rowFilter = rowFilter;

        log = cctx.kernalContext().log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void visit(SchemaIndexCacheVisitorClosure clo) {
        assert nonNull(clo);

        List<GridDhtLocalPartition> locParts = cctx.topology().localPartitions();

        if (locParts.isEmpty()) {
            buildIdxFut.onDone();

            return;
        }

        cctx.group().metrics().addIndexBuildCountPartitionsLeft(locParts.size());

        beforeExecute();

        AtomicInteger partsCnt = new AtomicInteger(locParts.size());

        AtomicBoolean stop = new AtomicBoolean();

        GridCompoundFuture<Void, Void> buildIdxCompoundFut = new GridCompoundFuture<>();

        for (GridDhtLocalPartition locPart : locParts) {
            GridWorkerFuture<Void> workerFut = new GridWorkerFuture<>();

            GridWorker worker = new SchemaIndexCachePartitionWorker(
                cctx,
                locPart,
                stop,
                cancel,
                clo,
                workerFut,
                rowFilter,
                partsCnt
            );

            workerFut.setWorker(worker);
            buildIdxCompoundFut.add(workerFut);

            cctx.kernalContext().buildIndexExecutorService().execute(worker);
        }

        buildIdxCompoundFut.listen(fut -> buildIdxFut.onDone(fut.error()));

        buildIdxCompoundFut.markInitialized();
    }

    /**
     * This method is called before creating or rebuilding indexes.
     * Used only for test.
     */
    protected void beforeExecute(){
        //no-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCacheVisitorImpl.class, this);
    }
}
