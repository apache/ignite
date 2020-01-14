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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;

/**
 *
 */
public class AbstractExecutionTest extends GridCommonAbstractTest {
    /** */
    protected BypassExchangeService exch;

    /** */
    protected Map<UUID, ExecutorService> executors;

    /** */
    protected volatile Throwable lastException;

    @Before
    public void setup() {
        executors = new ConcurrentHashMap<>();

        exch = new BypassExchangeService(executors, log());
    }

    @After
    public void tearDown() throws Throwable {
        for (ExecutorService executor : executors.values())
            U.shutdownNow(getClass(), executor, log());

        if (lastException != null)
            throw lastException;
    }

    /** */
    protected ExecutionContext executionContext(UUID nodeId, UUID queryId, long fragmentId) {
        ExecutorService exec = executors.computeIfAbsent(nodeId, id -> Executors.newSingleThreadExecutor());

        return new ExecutionContext(IgniteCalciteContext.builder()
            .localNodeId(nodeId)
            .taskExecutor((qid, fid, t) -> CompletableFuture.runAsync(t, exec).exceptionally(this::handle))
            .exchangeService(exch)
            .mailboxRegistry(exch)
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
