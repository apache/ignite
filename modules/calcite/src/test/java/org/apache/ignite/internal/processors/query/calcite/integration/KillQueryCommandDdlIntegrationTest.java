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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_VIEW;

/**
 * Tests `KILL QUERY` command.
 */
@RunWith(Parameterized.class)
public class KillQueryCommandDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;

    /** If {@code true}, cancel asynchronously. */
    @Parameterized.Parameter(0)
    public boolean isAsync;

    /** If {@code true}, cancel on client(initiator), otherwise on server. */
    @Parameterized.Parameter(1)
    public boolean cancelOnClient;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "isAsync={0},cancelOnClient={1}")
    public static Collection<?> parameters() {
        return Stream.of(true, false).flatMap(p -> Stream.of(new Object[] {p, true}, new Object[] {p, false}))
            .collect(Collectors.toList());
    }

    /** */
    @Test
    public void testCancelUnknownSqlQuery() {
        IgniteEx srv = grid(0);
        UUID nodeId = cancelOnClient ? client.localNode().id() : srv.localNode().id();
        Long qryId = ThreadLocalRandom.current().nextLong(10, 10000);
        GridTestUtils.assertThrows(log, () -> {
                sql(cancelOnClient ? client : srv, "KILL QUERY" + (isAsync ? " ASYNC '" : " '") + nodeId + "_"
                    + qryId + "'");
            },
            IgniteException.class,
            String.format("Failed to cancel query [nodeId=%s, qryId=%d, err=Query with provided ID doesn't exist " +
                    "[nodeId=%s, qryId=%d]]", nodeId, qryId, nodeId, qryId)
        );
    }

    /** */
    @Test
    public void testCancelSqlQuery() throws Exception {
        IgniteEx srv = grid(0);
        CalciteQueryProcessor srvEngine = queryProcessor(srv);

        sql("CREATE TABLE person (id int, val varchar)");
        sql("INSERT INTO person (id, val) VALUES (?, ?)", 0, "val0");

        IgniteCacheTable oldTbl = (IgniteCacheTable)srvEngine.schemaHolder().schema("PUBLIC").getTable("PERSON");

        CountDownLatch scanLatch = new CountDownLatch(1);
        AtomicBoolean stop = new AtomicBoolean();

        IgniteCacheTable newTbl = new CacheTableImpl(srv.context(), oldTbl.descriptor()) {
            @Override public <Row> Iterable<Row> scan(
                ExecutionContext<Row> execCtx,
                ColocationGroup grp,
                @Nullable ImmutableBitSet usedColumns
            ) {
                return new Iterable<Row>() {
                    @NotNull @Override public Iterator<Row> iterator() {
                        scanLatch.countDown();

                        return new Iterator<Row>() {
                            @Override public boolean hasNext() {
                                // Produce rows until stopped.
                                return !stop.get();
                            }

                            @Override public Row next() {
                                if (stop.get())
                                    throw new NoSuchElementException();

                                return execCtx.rowHandler().factory().create();
                            }
                        };
                    }
                };
            }
        };

        srvEngine.schemaHolder().schema("PUBLIC").add("PERSON", newTbl);

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql("SELECT * FROM person"));

        try {
            scanLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);

            SystemView<SqlQueryView> srvView = srv.context().systemView().view(SQL_QRY_VIEW);
            assertTrue(F.isEmpty(srvView)); // Register query in running query manager only on initiator.

            SystemView<SqlQueryView> clientView = client.context().systemView().view(SQL_QRY_VIEW);
            assertFalse(F.isEmpty(clientView));

            String clientQryId = F.first(clientView).queryId();

            GridTestUtils.runAsync(() -> sql(cancelOnClient ? client : srv,
                "KILL QUERY" + (isAsync ? " ASYNC '" : " '") + clientQryId + "'"));

            assertTrue(GridTestUtils.waitForCondition(() -> F.isEmpty(srvView), TIMEOUT));
            assertTrue(GridTestUtils.waitForCondition(() -> F.isEmpty(clientView), TIMEOUT));
        }
        finally {
            stop.set(true);
        }

        GridTestUtils.assertThrowsAnyCause(log,
            () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }
}
