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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.internal.processors.query.calcite.exchange.BypassExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.exchange.ExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ExecutionTest extends GridCommonAbstractTest {
    /** */
    private ExchangeProcessor exch;

    private List<ExecutorService> executors;

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

    @Test
    public void testSimpleExecution() throws Exception {
        // SELECT P.ID, P.NAME, PR.NAME AS PROJECT
        // FROM PERSON P
        // INNER JOIN PROJECT PR
        // ON P.ID = PR.RESP_ID
        // WHERE P.ID >= 2

        ExecutionContext ctx = executionContext(UUID.randomUUID());

        ScanNode persons = new ScanNode(ctx, Arrays.asList(
            new Object[]{0, "Igor", "Seliverstov"},
            new Object[]{1, "Roman", "Kondakov"},
            new Object[]{2, "Ivan", "Pavlukhin"},
            new Object[]{3, "Alexey", "Goncharuk"}
        ));

        ScanNode projects = new ScanNode(ctx, Arrays.asList(
            new Object[]{0, 2, "Calcite"},
            new Object[]{1, 1, "SQL"},
            new Object[]{2, 2, "Ignite"},
            new Object[]{3, 0, "Core"}
        ));

        JoinNode join = new JoinNode(ctx, persons, projects, (r1, r2) -> r1[0] != r2[1] ? null : new Object[]{r1[0], r1[1], r1[2], r2[0], r2[1], r2[2]});
        ProjectNode project = new ProjectNode(ctx, join, r -> new Object[]{r[0], r[1], r[5]});
        FilterNode filter = new FilterNode(ctx, project, r -> (Integer) r[0] >= 2);
        ConsumerNode node = new ConsumerNode(ctx, filter, 1);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Calcite"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Ignite"}, rows.get(1));
    }

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
