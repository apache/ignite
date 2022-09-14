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

package org.apache.ignite.stream.pubsub;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mock Pub/Sub Server
 */
class MockPubSubServer {
    /** Test topic. */
    public static final String TOPIC_NAME = "pagevisits";

    /** */
    public static final String PROJECT = "test-project";

    /** */
    private static final String LOCALHOST = "localhost";

    /** */
    private static final int PORT = 8080;

    /** */
    public static final int MESSAGES_PER_REQUEST = 10;

    /** Time to wait for the message in milliseconds. */
    private static final long MSG_WAIT_TIMEOUT = 1_000L;

    /** */
    private final Map<String, Publisher> publishers = new HashMap<>();

    /** */
    private final BlockingDeque<PubsubMessage> blockingQueue = new LinkedBlockingDeque<>();

    /**
     * @return Subscriber settings.
     * @throws IOException If fails.
     */
    public SubscriberStubSettings createSubscriberStub() throws IOException {
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        ManagedChannel managedChannel = managedChannel();

        FixedTransportChannelProvider transportChannel = FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel));
        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
            .setTransportChannelProvider(transportChannel)
            .setCredentialsProvider(credentialsProvider)
            .build();
        return subscriberStubSettings;
    }

    /** */
    @NotNull
    private ManagedChannel managedChannel() {
        ManagedChannel managedChannel = Mockito.mock(ManagedChannel.class);
        when(managedChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class))).thenAnswer((la) -> {
            MethodDescriptor methodDescriptor = (MethodDescriptor)la.getArguments()[0];
            if (methodDescriptor.getFullMethodName().equals("google.pubsub.v1.Subscriber/Acknowledge")) {
                return acknowledgeCall();
            }

            return clientCall();
        });
        return managedChannel;
    }

    /** */
    private ClientCall<AcknowledgeRequest, Empty> acknowledgeCall() {
        ClientCall<AcknowledgeRequest, Empty> clientCall = Mockito.mock(ClientCall.class);
        doAnswer(iom -> {
                Object[] arguments = iom.getArguments();
                ClientCall.Listener<Empty> listener = (ClientCall.Listener<Empty>)arguments[0];
                listener.onMessage(Empty.getDefaultInstance());
                Metadata metadata = (Metadata)arguments[1];
                listener.onClose(Status.OK, metadata);
                return null;
            }
        ).when(clientCall).start(any(ClientCall.Listener.class), any(Metadata.class));
        return clientCall;
    }

    /** */
    private ClientCall<PullRequest, PullResponse> clientCall() {
        ClientCall<PullRequest, PullResponse> clientCall = Mockito.mock(ClientCall.class);

        doAnswer(
            iom -> {
                Object[] arguments = iom.getArguments();
                ClientCall.Listener<PullResponse> listener = (ClientCall.Listener<PullResponse>)arguments[0];
                Metadata metadata = (Metadata)arguments[1];
                pullMessages(listener, metadata);
                return null;
            }
        ).when(clientCall).start(any(ClientCall.Listener.class), any(Metadata.class));
        return clientCall;
    }

    /** */
    private void pullMessages(ClientCall.Listener<PullResponse> listener, Metadata metadata) {
        PullResponse.Builder pullResponse = PullResponse.newBuilder();

        try {
            for (int i = 0; i < MESSAGES_PER_REQUEST; i++) {
                PubsubMessage msg = blockingQueue.poll(MSG_WAIT_TIMEOUT, TimeUnit.MILLISECONDS);

                if (msg == null)
                    break;

                pullResponse.addReceivedMessages(ReceivedMessage.newBuilder().mergeMessage(msg).build());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        listener.onMessage(pullResponse.build());
        listener.onClose(Status.OK, metadata);
    }

    /**
     * @param topicName Topic name.
     * @return Publisher.
     * @throws IOException If fails.
     */
    public Publisher getPublisher(String topicName) throws IOException {
        publishers.putIfAbsent(topicName, createPublisher(topicName));
        return publishers.get(topicName);
    }

    /** */
    private Publisher createPublisher(String topic) {
        Publisher publisher = mock(Publisher.class);

        when(publisher.publish(any(PubsubMessage.class))).thenAnswer(
            (iom) -> {
                PubsubMessage pubsubMessage = (PubsubMessage)iom.getArguments()[0];
                blockingQueue.add(pubsubMessage);
                return ApiFutures.immediateFuture(UUID.randomUUID().toString());
            }
        );
        return publisher;
    }

    /**
     * Obtains Pub/Sub address.
     *
     * @return Pub/Sub address.
     */
    private String getPubSubAddress() {
        return LOCALHOST + ":" + PORT;
    }
}
