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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.internal.processors.query.calcite.exchange.BypassExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.exchange.ExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class ContinuousExecutionTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public int rowsCount;

    /** */
    private ExchangeProcessor exch;

    /** */
    private List<ExecutorService> executors;

    /** */
    @Before
    public void setup() {
        exch = new BypassExchangeProcessor(log());
        executors = new ArrayList<>();
    }

    /** */
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

    @Parameterized.Parameters(name = "rowsCount={0}")
    public static List<Object[]> parameters() {
        return ImmutableList.of(
            new Object[]{10},
            new Object[]{100},
            new Object[]{100_000},
            new Object[]{1000_000});
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousExecution() throws Exception {
        UUID queryId = UUID.randomUUID();

        ExecutionContext ctx1 = executionContext(queryId);

        Iterable<Object[]> iterable = () -> new Iterator<Object[]>() {
            /** */
            private int cntr;

            /** */
            private final Random rnd = new Random();

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cntr < rowsCount;
            }

            /** {@inheritDoc} */
            @Override public Object[] next() {
                if (cntr >= rowsCount)
                    throw new NoSuchElementException();

                Object[] row = new Object[6];

                for (int i = 0; i < row.length; i++)
                    row[i] = rnd.nextInt(10);

                cntr++;

                return row;
            }
        };

        List<UUID> nodes = Collections.singletonList(UUID.randomUUID());

        ScanNode scan = new ScanNode(ctx1, iterable);
        ProjectNode project = new ProjectNode(ctx1, scan, r -> new Object[]{r[0], r[1], r[5]});
        FilterNode filter = new FilterNode(ctx1, project, r -> (Integer) r[0] >= 2);

        Outbox<Object[]> outbox = new Outbox<>(ctx1, 0, filter, new DestinationFunction() {
            @Override public List<UUID> destination(Object row) {
                return nodes;
            }

            @Override public List<UUID> targets() {
                return nodes;
            }
        });

        outbox.request();

        ExecutionContext ctx2 = executionContext(queryId);

        Inbox<Object[]> inbox = (Inbox<Object[]>) ctx2.plannerContext().exchangeProcessor().register(new Inbox<>(ctx2, 0));

        inbox.init(ctx2, nodes, null);

        ConsumerNode node = new ConsumerNode(ctx2, inbox);

        while (node.hasNext()) {
            Object[] row = node.next();

            assertTrue((Integer)row[0] >= 2);
        }
    }

    /** */
    private ExecutionContext executionContext(UUID queryId) {
        ExecutorService exec = Executors.newSingleThreadExecutor();

        executors.add(exec);

        return new ExecutionContext(queryId, PlannerContext.builder()
            .executor((t, id) -> CompletableFuture
                .runAsync(t, exec)
                .exceptionally((ex) -> {
                    log().error(ex.getMessage(), ex);

                    return null;
                }))
            .exchangeProcessor(exch)
            .logger(log())
            .build(), ImmutableMap.of());
    }
}
