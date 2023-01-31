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
import org.apache.ignite.client.monitoring.AuthenticationFailEvent;
import org.apache.ignite.client.monitoring.ConnectionClosedEvent;
import org.apache.ignite.client.monitoring.ConnectionEventListener;
import org.apache.ignite.client.monitoring.HandshakeFailEvent;
import org.apache.ignite.client.monitoring.HandshakeStartEvent;
import org.apache.ignite.client.monitoring.HandshakeSuccessEvent;
import org.apache.ignite.client.monitoring.QueryEventListener;
import org.apache.ignite.client.monitoring.QueryFailEvent;
import org.apache.ignite.client.monitoring.QueryStartEvent;
import org.apache.ignite.client.monitoring.QuerySuccessEvent;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.NullLogger;

/**
 * Routes events to listeners, specified in the client configuration.
 */
public class EventListenerDemultiplexer implements QueryEventListener, ConnectionEventListener {
    /** Noop listener. */
    private static final EventListenerDemultiplexer NO_OP = new EventListenerDemultiplexer();

    /** */
    final List<QueryEventListener> qryEventListeners;

    /** */
    final List<ConnectionEventListener> connEventListeners;

    /** */
    final IgniteLogger logger;

    /** */
    EventListenerDemultiplexer() {
        qryEventListeners = null;
        connEventListeners = null;
        logger = NullLogger.INSTANCE;
    }

    /** */
    EventListenerDemultiplexer(
        List<QueryEventListener> qryEventListeners,
        List<ConnectionEventListener> connEventListeners,
        IgniteLogger logger
    ) {
        this.logger = logger;

        if (!F.isEmpty(qryEventListeners))
            this.qryEventListeners = Collections.unmodifiableList(qryEventListeners);
        else
            this.qryEventListeners = null;

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

        List<QueryEventListener> qryEventListeners = new ArrayList<>();
        List<ConnectionEventListener> connEventListeners = new ArrayList<>();

        for (EventListener l: cfg.getEventListeners()) {
            if (l instanceof QueryEventListener)
                qryEventListeners.add((QueryEventListener)l);
            else if (l instanceof ConnectionEventListener)
                connEventListeners.add((ConnectionEventListener)l);
        }

        if (F.isEmpty(qryEventListeners) && F.isEmpty(connEventListeners))
            return NO_OP;

        return new EventListenerDemultiplexer(qryEventListeners, connEventListeners, NullLogger.whenNull(cfg.getLogger()));
    }

    /** {@inheritDoc} */
    @Override public void onQueryStart(QueryStartEvent event) {
        executeForEach(qryEventListeners, l -> l.onQueryStart(event));
    }

    /** {@inheritDoc} */
    @Override public void onQuerySuccess(QuerySuccessEvent event) {
        executeForEach(qryEventListeners, l -> l.onQuerySuccess(event));
    }

    /** {@inheritDoc} */
    @Override public void onQueryFail(QueryFailEvent event) {
        executeForEach(qryEventListeners, l -> l.onQueryFail(event));
    }

    /** {@inheritDoc} */
    @Override public void onHandshakeStart(HandshakeStartEvent event) {
        executeForEach(connEventListeners, l -> l.onHandshakeStart(event));
    }

    /** {@inheritDoc} */
    @Override public void onHandshakeSuccess(HandshakeSuccessEvent event) {
        executeForEach(connEventListeners, l -> l.onHandshakeSuccess(event));
    }

    /** {@inheritDoc} */
    @Override public void onHandshakeFail(HandshakeFailEvent event) {
        executeForEach(connEventListeners, l -> l.onHandshakeFail(event));
    }

    /** {@inheritDoc} */
    @Override public void onAuthenticationFail(AuthenticationFailEvent event) {
        executeForEach(connEventListeners, l -> l.onAuthenticationFail(event));
    }

    /** {@inheritDoc} */
    @Override public void onConnectionClosed(ConnectionClosedEvent event) {
        executeForEach(connEventListeners, l -> l.onConnectionClosed(event));
    }

    /** */
    private <T> void executeForEach(List<T> listeners, Consumer<T> action) {
        if (F.isEmpty(listeners))
            return;

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
