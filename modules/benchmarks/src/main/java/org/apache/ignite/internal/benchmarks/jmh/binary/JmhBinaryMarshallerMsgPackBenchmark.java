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

package org.apache.ignite.internal.benchmarks.jmh.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Ignite marshaller vs MsgPack benchmark.
 */
@State(Scope.Benchmark)
public class JmhBinaryMarshallerMsgPackBenchmark extends JmhAbstractBenchmark {
    private BinaryMarshaller marshaller;

    /**
     * Setup routine. Child classes must invoke this method first.
     *
     * @throws Exception If failed.
     */
    @Setup
    public void setup() throws Exception {
        System.out.println();
        System.out.println("--------------------");

        marshaller = createBinaryMarshaller(new NullLogger());
    }

    @Benchmark
    public void writeIgnite() throws IgniteCheckedException {
        marshaller.marshal(new IntValue(randomInt()));
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner runner = JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(4)
                .benchmarks(JmhBinaryMarshallerMsgPackBenchmark.class.getSimpleName())
                .jvmArguments("-Xms4g", "-Xmx4g");

        runner
                .benchmarkModes(Mode.Throughput)
                .run();
    }

    private BinaryMarshaller createBinaryMarshaller(IgniteLogger log) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration()
                .setBinaryConfiguration(
                        new BinaryConfiguration().setCompactFooter(true)
                )
                .setClientMode(false)
                .setDiscoverySpi(new TcpDiscoverySpi() {
                    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                        //No-op.
                    }
                });

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), iCfg, new NullLogger());

        MarshallerContextBenchImpl marshCtx = new MarshallerContextBenchImpl();

        marshCtx.onMarshallerProcessorStarted(new GridBenchKernalContext(log, iCfg), null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(marshCtx);

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
    }
}
