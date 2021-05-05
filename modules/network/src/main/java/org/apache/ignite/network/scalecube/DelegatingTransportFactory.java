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
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class is a hack designed to handle a single use-case: sending a message from a node to itself. In all other
 * cases it should behave as the default ScaleCube transport factory.
 */
class DelegatingTransportFactory implements TransportFactory {
    /** */
    private final ScaleCubeMessagingService messagingService;

    /**
     * @param messagingService Messaging service.
     */
    DelegatingTransportFactory(ScaleCubeMessagingService messagingService) {
        this.messagingService = messagingService;
    }

    /** {@inheritDoc} */
    @Override public Transport createTransport(TransportConfig config) {
        var delegateFactory = TransportFactory.INSTANCE == null ? new TcpTransportFactory() : TransportFactory.INSTANCE;

        Transport delegate = delegateFactory.createTransport(config);

        return new Transport() {
            @Override public Address address() {
                return delegate.address();
            }

            @Override public Mono<Transport> start() {
                return delegate.start().thenReturn(this);
            }

            @Override public Mono<Void> stop() {
                return delegate.stop();
            }

            @Override public boolean isStopped() {
                return delegate.isStopped();
            }

            @Override public Mono<Void> send(Address address, Message message) {
                return delegate.send(address, message);
            }

            @Override public Flux<Message> listen() {
                return delegate.listen();
            }

            @Override public Mono<Message> requestResponse(Address address, Message request) {
                return address.equals(address()) ?
                    requestResponseToSelf(request) :
                    delegate.requestResponse(address, request);
            }

            private Mono<Message> requestResponseToSelf(Message request) {
                Objects.requireNonNull(request, "request must be not null");
                Objects.requireNonNull(request.correlationId(), "correlationId must be not null");

                Mono<Message> result = listen()
                    .filter(resp -> resp.correlationId() != null)
                    .filter(resp -> resp.correlationId().equals(request.correlationId()))
                    .next();

                // manually fire the message event instead of sending it, because otherwise it will be received
                // immediately, replacing the response that might have been sent by the event handlers.
                messagingService.fireEvent(request);

                return result;
            }
        };
    }
}
