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

package org.apache.ignite.client.monitoring;

import java.util.EventListener;

/** */
public interface ConnectionEventListener extends EventListener {
    /**
     * @param event Handshake start event.
     */
    default void onHandshakeStart(HandshakeStartEvent event) {
        // No-op.
    }

    /**
     * @param event Handshake success event.
     */
    default void onHandshakeSuccess(HandshakeSuccessEvent event) {
        // No-op.
    }

    /**
     * @param event Handshake fail event.
     */
    default void onHandshakeFail(HandshakeFailEvent event) {
        // No-op.
    }

    /**
     * @param event Authentication fail event.
     */
    default void onAuthenticationFail(AuthenticationFailEvent event) {
        // No-op.
    }

    /**
     * @param event Connection lost event (exceptionally closed).
     */
    default void onConnectionLost(ConnectionLostEvent event) {
        // No-op.
    }

    /**
     * @param event Connection closed event (without exception).
     */
    default void onConnectionClosed(ConnectionClosedEvent event) {
        // No-op.
    }
}
