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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.List;
import com.google.common.collect.ImmutableMap;
import io.opencensus.trace.SpanId;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.tracing.SpanType.TX;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_COLOCATED_LOCK_MAP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_COMMIT;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_DHT_FINISH;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_DHT_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_ENLIST_WRITE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_FINISH_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_PREPARE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_PREPARE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_ROLLBACK;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests to check correctness of OpenCensus Transactions Tracing implementation.
 */
public class OpenCensusTxTracingTest extends AbstractTracingTest {

    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /** {@inheritDoc} */
    @Override public void before() throws Exception {
        super.before();

        grid(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());
    }

    /**
     * <ol>
     *     <li>Run pessimistic serializable transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testPessimisticSerializableTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(PESSIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", PESSIMISTIC.name())
                .put("isolation", SERIALIZABLE.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_COLOCATED_LOCK_MAP,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run optimistic serializable transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testOptimisticSerializableTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", SERIALIZABLE.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run pessimistic read-committed transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testPessimisticReadCommittedTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(PESSIMISTIC, READ_COMMITTED);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", PESSIMISTIC.name())
                .put("isolation", READ_COMMITTED.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_COLOCATED_LOCK_MAP,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run optimistic read-committed transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testOptimisticReadCommittedTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, READ_COMMITTED);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", READ_COMMITTED.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run pessimistic repeatable-read transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testPessimisticRepeatableReadTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(PESSIMISTIC, REPEATABLE_READ);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", PESSIMISTIC.name())
                .put("isolation", REPEATABLE_READ.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_COLOCATED_LOCK_MAP,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run optimistic repeatable-read transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testOptimisticRepeatableReadTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, REPEATABLE_READ);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", REPEATABLE_READ.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run some transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Rollback given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.rollback
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testRollbackTransaction() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, REPEATABLE_READ);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.rollback();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", REPEATABLE_READ.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_ROLLBACK,
            txSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run some transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Close given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.close
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testCloseTransaction() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, REPEATABLE_READ);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.close();

        handler().flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", REPEATABLE_READ.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_CLOSE,
            txSpanIds.get(0),
            1,
            null);
    }
}
