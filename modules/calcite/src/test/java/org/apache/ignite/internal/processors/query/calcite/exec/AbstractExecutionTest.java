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

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.internal.processors.query.calcite.exchange.BypassExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;

/**
 *
 */
public class AbstractExecutionTest extends GridCommonAbstractTest {
    /** */
    protected BypassExchangeProcessor exch;

    /** */
    protected List<ExecutorService> executors;

    /** */
    protected volatile Throwable lastException;

    @Before
    public void setup() {
        exch = new BypassExchangeProcessor(log());
        executors = new ArrayList<>();
    }

    @After
    public void tearDown() {
        for (ExecutorService executor : executors) {
            List<Runnable> runnables = executor.shutdownNow();

            for (Runnable runnable : runnables) {
                if (runnable instanceof Future)
                    ((Future<?>) runnable).cancel(true);
            }
        }
    }

    /** */
    protected ExecutionContext executionContext(UUID nodeId, UUID queryId, long fragmentId) {
        ExecutorService exec = Executors.newSingleThreadExecutor();

        executors.add(exec);

        return new ExecutionContext(IgniteCalciteContext.builder()
            .localNodeId(nodeId)
            .taskExecutor((qid, fid, t) -> CompletableFuture.runAsync(t, exec).exceptionally(this::handle))
            .exchangeProcessor(exch)
            .inboxRegistry(exch)
            .logger(log())
            .build(), queryId, fragmentId, null, ImmutableMap.of());
    }

    /** */
    private Void handle(Throwable ex) {
        log().error(ex.getMessage(), ex);

        lastException = ex;

        return null;
    }
}
