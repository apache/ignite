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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.Query;
import org.apache.ignite.internal.processors.query.calcite.QueryState;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartResponse;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests query cleanup when a fragment start response arrives after a DELETE failure. */
public class QueryCleanupRaceIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** Delayed start acknowledgement must not leave the failed DELETE registered on the server. */
    @Test
    void testDeleteFailureWithDelayedQueryStartResponse() throws Exception {
        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO test (id, val) VALUES (1, 0)");

        // The second execution exercises the cached-plan path, which has no list of sibling queries.
        for (int i = 0; i < 2; i++)
            checkDeleteFailureWithDelayedQueryStartResponse();
    }

    /** */
    private void checkDeleteFailureWithDelayedQueryStartResponse() throws Exception {
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(0));
        AtomicReference<UUID> qryId = new AtomicReference<>();

        // Setup queries have completed; only the following DELETE is executed while blocking is enabled.
        spi.blockMessages((node, msg) -> {
            if (node.id().equals(client.localNode().id()) && msg instanceof QueryStartResponse) {
                qryId.set(((QueryStartResponse)msg).queryId());

                return true;
            }

            return false;
        });

        try {
            // Error messages remain deliverable, so the caller observes the error before the start response.
            // Error sql must necessarily be executed through the rows of the table.
            assertThrows("DELETE FROM test WHERE 100 / val > 0", ArithmeticException.class, "/ by zero");

            assertTrue("No QueryStartResponse was intercepted", spi.waitForBlocked(1, 5_000));

            Query<?> root = queryProcessor(client).queryRegistry().query(qryId.get());

            assertNotNull("The query must remain registered while its start response is pending", root);
            assertEquals(QueryState.CLOSING, root.state());
        }
        finally {
            spi.stopBlock();
        }

        assertTrue("Failed DELETE is still registered on the server [queryId=" + qryId.get() + ']',
            waitForCondition(() -> queryProcessor(grid(0)).queryRegistry().query(qryId.get()) == null, 5_000));
        assertTrue("Failed DELETE is still registered on the client [queryId=" + qryId.get() + ']',
            waitForCondition(() -> queryProcessor(client).queryRegistry().query(qryId.get()) == null, 5_000));
    }

    /** A propagated execution error must not be added back to its own cause as a suppressed exception. */
    @Test
    void testExecutionFailureHasAcyclicExceptionGraph() {
        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO test (id, val) VALUES (1, 0)");

        for (int i = 0; i < 2; i++) {
            IgniteSQLException error = Assertions.assertThrows(IgniteSQLException.class,
                () -> sql("DELETE FROM test WHERE 100 / val > 0"));

            assertTrue(X.hasCause(error, ArithmeticException.class));
            assertNoExceptionCycle(error, Collections.newSetFromMap(new IdentityHashMap<>()));
            Assertions.assertDoesNotThrow(() -> new VisorExceptionWrapper(error));
        }
    }

    /** Check paths rather than all visited nodes: sharing a cause does not constitute a cycle. */
    private void assertNoExceptionCycle(Throwable error, Set<Throwable> ancestors) {
        if (error == null)
            return;

        assertTrue("Cycle in the cause/suppressed exception graph", ancestors.add(error));

        assertNoExceptionCycle(error.getCause(), ancestors);

        for (Throwable suppressed : error.getSuppressed())
            assertNoExceptionCycle(suppressed, ancestors);

        ancestors.remove(error);
    }

    /** Errors before creation of a root execution node must also unregister the query. */
    @Test
    void testPlanningFailureClosesQuery() {
        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        assertThrows("SELECT missing FROM test", IgniteSQLException.class, "not found");

        assertTrue(queryProcessor(client).queryRegistry().runningQueries().isEmpty());
    }

    /** A later statement's planning failure must close earlier statements in the same call. */
    @Test
    void testPlanningFailureClosesEarlierStatement() {
        sql("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO test (id, val) VALUES (1, 0)");

        assertThrows("SELECT * FROM test; SELECT missing FROM test", IgniteSQLException.class, "not found");
    }
}
