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

package org.apache.ignite.internal.benchmarks.jmh.direct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GroupPartitionIdPair;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.openjdk.jmh.annotations.Mode.Throughput;

/** Benchmarks the {@link DirectMessageReader} compressed-field hot path. */
@State(Scope.Thread)
@BenchmarkMode(Throughput)
@Warmup(iterations = 5, time = 3, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = SECONDS)
@Fork(1)
public class JmhDirectMessageReaderBenchmark {
    /** */
    @Param({"30", "500"})
    private int entries;

    /** */
    private DirectMessageReader reader;

    /** Fully serialized compressed message, as received from the network. */
    private ByteBuffer buf;

    /** */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .benchmarks(JmhDirectMessageReaderBenchmark.class.getName())
            .run();
    }

    /** */
    @Setup
    public void setup() {
        MessageFactory msgFactory = msgFactory();

        reader = new DirectMessageReader(msgFactory, null);

        Map<UUID, Map<GroupPartitionIdPair, Long>> partHistSuppliers = new HashMap<>();
        Map<UUID, Map<Integer, Set<Integer>>> partsToReload = new HashMap<>();

        for (int i = 0; i < entries; i++) {
            UUID nodeId = UUID.randomUUID();

            partHistSuppliers.put(nodeId, Map.of(new GroupPartitionIdPair(i, i + 1), i + 2L));
            partsToReload.put(nodeId, Map.of(i, Set.of(i + 1)));
        }

        GridDhtPartitionsFullMessage msg = new GridDhtPartitionsFullMessage(null, null,
            new AffinityTopologyVersion(0), partHistSuppliers, partsToReload);

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);

        buf = ByteBuffer.allocate(4 * 1024 * 1024);

        writer.setBuffer(buf);

        boolean finished = writer.writeMessage(msg, true);

        if (!finished)
            throw new IllegalStateException("Message does not fit into the buffer.");

        buf.flip();
    }

    /** Exchange-style compressed message deserialization. */
    @Benchmark
    public Message compressedMessage() {
        buf.rewind();

        reader.setBuffer(buf);

        Message msg = reader.readMessage(true);

        reader.reset();

        return msg;
    }

    /** */
    private static MessageFactory msgFactory() {
        return new IgniteMessageFactoryImpl(new MessageFactoryProvider[]{
            new CoreMessagesProvider(jdk(), jdk(), U.gridClassLoader())});
    }
}
