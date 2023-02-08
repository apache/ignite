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

package org.apache.ignite.internal.client.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.events.ConnectionClosedEvent;
import org.apache.ignite.client.events.ConnectionDescription;
import org.apache.ignite.client.events.ConnectionEventListener;
import org.apache.ignite.client.events.HandshakeFailEvent;
import org.apache.ignite.client.events.HandshakeStartEvent;
import org.apache.ignite.client.events.HandshakeSuccessEvent;
import org.apache.ignite.client.events.RequestEventListener;
import org.apache.ignite.client.events.RequestFailEvent;
import org.apache.ignite.client.events.RequestStartEvent;
import org.apache.ignite.client.events.RequestSuccessEvent;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.NullLogger;

/**
 * Routes events to listeners, specified in the client configuration.
 */
public class EventListenerDemultiplexer {
    /** Noop listener. */
    private static final EventListenerDemultiplexer NO_OP = new EventListenerDemultiplexer();

    /** */
    final List<RequestEventListener> reqEventListeners;

    /** */
    final List<ConnectionEventListener> connEventListeners;

    /** */
    final IgniteLogger logger;

    /** */
    EventListenerDemultiplexer() {
        reqEventListeners = null;
        connEventListeners = null;
        logger = NullLogger.INSTANCE;
    }

    /** */
    EventListenerDemultiplexer(
        List<RequestEventListener> reqEventListeners,
        List<ConnectionEventListener> connEventListeners,
        IgniteLogger logger
    ) {
        this.logger = logger;

        if (!F.isEmpty(reqEventListeners))
            this.reqEventListeners = Collections.unmodifiableList(reqEventListeners);
        else
            this.reqEventListeners = null;

        if (!F.isEmpty(connEventListeners))
            this.connEventListeners = Collections.unmodifiableList(connEventListeners);
        else
            this.connEventListeners = null;

    }

    /**
     * Creates an event listener demultiplexer.
     *
     * @param cfg Client configuration.
     */
    public static EventListenerDemultiplexer create(ClientConfiguration cfg) {
        if (F.isEmpty(cfg.getEventListeners()))
            return NO_OP;

        List<RequestEventListener> qryEventListeners = new ArrayList<>();
        List<ConnectionEventListener> connEventListeners = new ArrayList<>();

        for (EventListener l: cfg.getEventListeners()) {
            if (l instanceof RequestEventListener)
                qryEventListeners.add((RequestEventListener)l);
            else if (l instanceof ConnectionEventListener)
                connEventListeners.add((ConnectionEventListener)l);
        }

        if (F.isEmpty(qryEventListeners) && F.isEmpty(connEventListeners))
            return NO_OP;

        return new EventListenerDemultiplexer(qryEventListeners, connEventListeners, NullLogger.whenNull(cfg.getLogger()));
    }

    /**
     * @param conn Connection description.
     * @param requestId Request id.
     * @param opCode Operation code.
     * @param opName Operation name.
     */
    public void onRequestStart(ConnectionDescription conn, long requestId, short opCode, String opName) {
        if (F.isEmpty(reqEventListeners))
            return;

        executeForEach(reqEventListeners, l -> l.onRequestStart(new RequestStartEvent(conn, requestId, opCode, opName)));
    }

    /**
     * @param conn Connection description.
     * @param requestId Request id.
     * @param opCode Operation code.
     * @param opName Operation name.
     * @param elapsedTimeNanos Elapsed time in nanoseconds.
     */
    public void onRequestSuccess(
        ConnectionDescription conn,
        long requestId,
        short opCode,
        String opName,
        long elapsedTimeNanos
    ) {
        if (F.isEmpty(reqEventListeners))
            return;

        executeForEach(reqEventListeners, l ->
            l.onRequestSuccess(new RequestSuccessEvent(conn, requestId, opCode, opName, elapsedTimeNanos)));
    }

    /**
     * @param conn Connection description.
     * @param requestId Request id.
     * @param opCode Operation code.
     * @param opName Operation name.
     * @param elapsedTimeNanos Elapsed time in nanoseconds.
     * @param throwable Throwable that caused the failure.
     */
    public void onRequestFail(
        ConnectionDescription conn,
        long requestId,
        short opCode,
        String opName,
        long elapsedTimeNanos,
        Throwable throwable
    ) {
        if (F.isEmpty(reqEventListeners))
            return;

        executeForEach(reqEventListeners, l ->
            l.onRequestFail(new RequestFailEvent(conn, requestId, opCode, opName, elapsedTimeNanos, throwable)));
    }

    /**
     * @param conn Connection description.
     */
    public void onHandshakeStart(ConnectionDescription conn) {
        if (F.isEmpty(connEventListeners))
            return;

        executeForEach(connEventListeners, l -> l.onHandshakeStart(new HandshakeStartEvent(conn)));
    }

    /**
     * @param conn Connection description.
     * @param elapsedTimeNanos Elapsed time in nanoseconds.
     */
    public void onHandshakeSuccess(ConnectionDescription conn, long elapsedTimeNanos) {
        if (F.isEmpty(connEventListeners))
            return;

        executeForEach(connEventListeners, l -> l.onHandshakeSuccess(new HandshakeSuccessEvent(conn, elapsedTimeNanos)));
    }

    /**
     * @param conn Connection description.
     * @param elapsedTimeNanos Elapsed time in nanoseconds.
     * @param throwable Throwable that caused the failure.
     */
    public void onHandshakeFail(ConnectionDescription conn, long elapsedTimeNanos, Throwable throwable) {
        if (F.isEmpty(connEventListeners))
            return;

        executeForEach(connEventListeners, l -> l.onHandshakeFail(new HandshakeFailEvent(conn, elapsedTimeNanos, throwable)));
    }

    /**
     * @param conn Connection description.
     * @param throwable Throwable that caused the failure if any.
     */
    public void onConnectionClosed(ConnectionDescription conn, Throwable throwable) {
        if (F.isEmpty(connEventListeners))
            return;

        executeForEach(connEventListeners, l -> l.onConnectionClosed(new ConnectionClosedEvent(conn, throwable)));
    }

    /** */
    private <T> void executeForEach(List<T> listeners, Consumer<T> action) {
        assert !F.isEmpty(listeners);

        for (T listener: listeners) {
            try {
                action.accept(listener);
            }
            catch (Exception e) {
                logger.warning("Exception thrown while consuming event in listener " + listener, e);
            }
        }
    }
}
