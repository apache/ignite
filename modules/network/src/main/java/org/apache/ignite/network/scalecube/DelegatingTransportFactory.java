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

package org.apache.ignite.network.scalecube;

import java.util.Objects;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.net.Address;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class is a hack designed to handle a single use-case: sending a message from a node to itself. In all other
 * cases it should behave as the default ScaleCube transport factory.
 */
class DelegatingTransportFactory implements TransportFactory {
    /** */
    private final ScaleCubeMessagingService messagingService;

    /** Delegate transport factory. */
    private final TransportFactory factory;

    /**
     * @param messagingService Messaging service.
     * @param factory Delegate transport factory.
     */
    DelegatingTransportFactory(ScaleCubeMessagingService messagingService, TransportFactory factory) {
        this.messagingService = messagingService;
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public Transport createTransport(TransportConfig config) {
        Transport delegate = factory.createTransport(config);

        return new Transport() {
            /** {@inheritDoc} */
            @Override public Address address() {
                return delegate.address();
            }

            /** {@inheritDoc} */
            @Override public Mono<Transport> start() {
                return delegate.start().thenReturn(this);
            }

            /** {@inheritDoc} */
            @Override public Mono<Void> stop() {
                return delegate.stop();
            }

            /** {@inheritDoc} */
            @Override public boolean isStopped() {
                return delegate.isStopped();
            }

            /** {@inheritDoc} */
            @Override public Mono<Void> send(Address address, Message message) {
                return delegate.send(address, message);
            }

            /** {@inheritDoc} */
            @Override public Flux<Message> listen() {
                return delegate.listen();
            }

            /** {@inheritDoc} */
            @Override public Mono<Message> requestResponse(Address address, Message request) {
                return address.equals(address()) ?
                    requestResponseToSelf(request) :
                    delegate.requestResponse(address, request);
            }

            /**
             * Send a request from this node to this node.
             *
             * @param request Message.
             * @return {@link Mono} with a response.
             */
            private Mono<Message> requestResponseToSelf(Message request) {
                Objects.requireNonNull(request, "request must be not null");
                Objects.requireNonNull(request.correlationId(), "correlationId must be not null");

                return Mono.create(sink -> {
                    // listen() returns a lazy Flux, so we need to subscribe to it first, using a sink.
                    // Otherwise, message handlers can execute faster than the consumer will be able to start listening
                    // to incoming messages.
                    Disposable disposable = listen()
                        .filter(resp -> resp.correlationId() != null)
                        .filter(resp -> resp.correlationId().equals(request.correlationId()))
                        .take(1)
                        .subscribe(sink::success, sink::error, sink::success);

                    // cancel the nested subscription if the client cancels the outer Mono
                    sink.onDispose(disposable);

                    // manually fire the message event instead of sending it, because otherwise it will be received
                    // immediately, replacing the response that might have been sent by the event handlers.
                    messagingService.fireEvent(request);
                });
            }
        };
    }
}
