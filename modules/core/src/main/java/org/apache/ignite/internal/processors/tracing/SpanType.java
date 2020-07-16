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

package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.spi.tracing.Scope;

/**
 * List of span type names used in appropriate sub-systems.
 */
public enum SpanType {
    // Discovery traces.
    /** Node join request. */
    DISCOVERY_NODE_JOIN_REQUEST(Scope.DISCOVERY, "discovery.node.join.request", 1, true),

    /** Node join add. */
    DISCOVERY_NODE_JOIN_ADD(Scope.DISCOVERY, "discovery.node.join.add", 2),

    /** Node join finish. */
    DISCOVERY_NODE_JOIN_FINISH(Scope.DISCOVERY, "discovery.node.join.finish", 3),

    /** Node failed. */
    DISCOVERY_NODE_FAILED(Scope.DISCOVERY, "discovery.node.failed", 4, true),

    /** Node left. */
    DISCOVERY_NODE_LEFT(Scope.DISCOVERY, "discovery.node.left", 5, true),

    /** Custom event. */
    DISCOVERY_CUSTOM_EVENT(Scope.DISCOVERY, "discovery.custom.event", 6, true),

    /** Exchange future. */
    EXCHANGE_FUTURE(Scope.DISCOVERY, "exchange.future", 7),

    /** Affinity calculation. */
    AFFINITY_CALCULATION(Scope.DISCOVERY, "affinity.calculation", 8),

    // Communication traces.
    /** Job execution request. */
    COMMUNICATION_JOB_EXECUTE_REQUEST(Scope.COMMUNICATION, "communication.job.execute.request", 9),

    /** Job execution response. */
    COMMUNICATION_JOB_EXECUTE_RESPONSE(Scope.COMMUNICATION, "communication.job.execute.response", 10),

    /** Socket write action. */
    COMMUNICATION_SOCKET_WRITE(Scope.COMMUNICATION, "socket.write", 11, true),

    /** Socket read action. */
    COMMUNICATION_SOCKET_READ(Scope.COMMUNICATION, "socket.read", 12),

    /** Process regular. */
    COMMUNICATION_REGULAR_PROCESS(Scope.COMMUNICATION, "process.regular", 13),

    /** Process ordered. */
    COMMUNICATION_ORDERED_PROCESS(Scope.COMMUNICATION, "process.ordered", 14),

    // Tx traces.
    /** Transaction start. */
    TX(Scope.TX, "transaction", 15, true),

    /** Transaction commit. */
    TX_COMMIT(Scope.TX, "transactions.commit", 16),

    /** Transaction rollback. */
    TX_ROLLBACK(Scope.TX, "transactions.rollback", 17),

    /** Transaction close. */
    TX_CLOSE(Scope.TX, "transactions.close", 18),

    /** Transaction suspend. */
    TX_SUSPEND(Scope.TX, "transactions.suspend", 19),

    /** Transaction resume. */
    TX_RESUME(Scope.TX, "transactions.resume", 20),

    /** Transaction near prepare. */
    TX_NEAR_PREPARE(Scope.TX, "transactions.near.prepare", 21),

    /** Transaction near prepare ondone. */
    TX_NEAR_PREPARE_ON_DONE(Scope.TX, "transactions.near.prepare.ondone", 22),

    /** Transaction near prepare onerror. */
    TX_NEAR_PREPARE_ON_ERROR(Scope.TX, "transactions.near.prepare.onerror", 23),

    /** Transaction near prepare ontimeout. */
    TX_NEAR_PREPARE_ON_TIMEOUT(Scope.TX, "transactions.near.prepare.ontimeout", 24),

    /** Transaction dht prepare. */
    TX_DHT_PREPARE(Scope.TX, "transactions.dht.prepare", 25),

    /** Transaction dht prepare ondone. */
    TX_DHT_PREPARE_ON_DONE(Scope.TX, "transactions.dht.prepare.ondone", 26),

    /** Transaction near finish. */
    TX_NEAR_FINISH(Scope.TX, "transactions.near.finish", 27),

    /** Transaction near finish ondone. */
    TX_NEAR_FINISH_ON_DONE(Scope.TX, "transactions.near.finish.ondone", 28),

    /** Transaction dht finish. */
    TX_DHT_FINISH(Scope.TX, "transactions.dht.finish", 29),

    /** Transaction dht finish ondone. */
    TX_DHT_FINISH_ON_DONE(Scope.TX, "transactions.dht.finish.ondone", 30),

    /** Transaction map proceed. */
    TX_MAP_PROCEED(Scope.TX, "transactions.lock.map.proceed", 31),

    /** Transaction map proceed. */
    TX_COLOCATED_LOCK_MAP(Scope.TX, "transactions.colocated.lock.map", 32),

    /** Transaction lock map. */
    TX_DHT_LOCK_MAP(Scope.TX, "transactions.dht.lock.map", 33),

    /** Transaction near enlist read. */
    TX_NEAR_ENLIST_READ(Scope.TX, "transactions.near.enlist.read", 34),

    /** Transaction near enlist write. */
    TX_NEAR_ENLIST_WRITE(Scope.TX, "transactions.near.enlist.write", 35),

    /** Transaction dht process prepare request. */
    TX_PROCESS_DHT_PREPARE_REQ(Scope.TX, "tx.dht.process.prepare.req", 36),

    /** Transaction dht process finish request. */
    TX_PROCESS_DHT_FINISH_REQ(Scope.TX, "tx.dht.process.finish.req", 37),

    /** Transaction dht finish response. */
    TX_PROCESS_DHT_FINISH_RESP(Scope.TX, "tx.dht.process.finish.resp", 38),

    /** Transaction dht one phase commit ack request. */
    TX_PROCESS_DHT_ONE_PHASE_COMMIT_ACK_REQ(Scope.TX, "tx.dht.process.one-phase-commit-ack.req", 39),

    /** Transaction dht prepare response. */
    TX_PROCESS_DHT_PREPARE_RESP(Scope.TX, "tx.dht.process.prepare.response", 40),

    /** Transaction near finish request. */
    TX_NEAR_FINISH_REQ(Scope.TX, "tx.near.process.finish.request", 41),

    /** Transaction near finish  response. */
    TX_NEAR_FINISH_RESP(Scope.TX, "tx.near.process.finish.response", 42),

    /** Transaction near prepare request. */
    TX_NEAR_PREPARE_REQ(Scope.TX, "tx.near.process.prepare.request", 43),

    /** Transaction near prepare  response. */
    TX_NEAR_PREPARE_RESP(Scope.TX, "tx.near.process.prepare.response", 44),

    /** Custom job call. */
    CUSTOM_JOB_CALL(Scope.COMMUNICATION, "job.call", 45, true);

    /** Scope */
    private Scope scope;

    /** Trace name. */
    private String spanName;

    /** Index. */
    private int idx;

    /** Values. */
    private static final SpanType[] VALS;

    /** {@code true} if given span is a root span within it's scope. */
    private boolean rootSpan;

    /**
     * Constructor.
     *
     * @param scope Scope.
     * @param spanName Span name.
     * @param idx Index.
     */
    SpanType(Scope scope, String spanName, int idx) {
        this.scope = scope;
        this.spanName = spanName;
        this.idx = idx;
    }

    /**
     * Constructor.
     *
     * @param scope Scope.
     * @param spanName Span name.
     * @param idx Index.
     * @param rootSpan Boolean flag, that indicates whether given span is root within it's scope or not.
     */
    SpanType(Scope scope, String spanName, int idx, boolean rootSpan) {
        this(scope, spanName, idx);
        this.rootSpan = rootSpan;
    }

    /**
     * @return Scope.
     */
    public Scope scope() {
        return scope;
    }

    /**
     * @return Trace name.
     */
    public String spanName() {
        return spanName;
    }

    /**
     * @return idx.
     */
    public int index() {
        return idx;
    }

    /**
     * @return Root span.
     */
    public boolean rootSpan() {
        return rootSpan;
    }

    static {
        SpanType[] spanTypes = SpanType.values();

        int maxIdx = 0;

        for (SpanType spanType : spanTypes)
            maxIdx = Math.max(maxIdx, spanType.idx);

        VALS = new SpanType[maxIdx + 1];

        for (SpanType spanType : spanTypes)
            VALS[spanType.idx] = spanType;
    }

    /**
     * @param idx Index.
     * @return Enum instance based on specified index.
     */
    public static SpanType fromIndex(int idx) {
        return idx < 0 || idx >= VALS.length ? null : VALS[idx];
    }
}
