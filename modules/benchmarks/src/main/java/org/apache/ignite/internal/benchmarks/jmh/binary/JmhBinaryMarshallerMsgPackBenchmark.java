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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.ByteArrayOutputStream;

/**
 * Ignite marshaller vs MsgPack benchmark.
 * Benchmark                                                       Mode  Cnt         Score        Error  Units
 * JmhBinaryMarshallerMsgPackBenchmark.writePrimitivesMsgPack     thrpt   10   7709666.338 ±  30310.271  ops/s // Unpooled (allocates buffers)
 * JmhBinaryMarshallerMsgPackBenchmark.writePrimitivesMsgPackRaw  thrpt   10  20952908.222 ±  93921.333  ops/s // Without true array copy
 * JmhBinaryMarshallerMsgPackBenchmark.writePrimitivesMsgPackRaw  thrpt   10  16834154.556 ± 85624.143  ops/s // With true array copy
 * JmhBinaryMarshallerMsgPackBenchmark.writePrimitivesIgnite      thrpt   10  12702562.838 ± 248094.068  ops/s
 * TODO: Read benchmarks.
 */
@State(Scope.Benchmark)
public class JmhBinaryMarshallerMsgPackBenchmark extends JmhAbstractBenchmark {
    private static final MessagePack.PackerConfig packerConfig = new MessagePack.PackerConfig().withBufferSize(128);

    private static final PooledMessageBufferOutput msgPackPooledOutput = new PooledMessageBufferOutput();

    private BinaryMarshaller marshaller;

    private ObjectMapper msgPackMapper;

    private ObjectWriter msgPackWriter;

    private BinaryContext binaryCtx;

    /**
     * Setup routine. Child classes must invoke this method first.
     *
     * @throws Exception If failed.
     */
    @Setup
    public void setup() throws Exception {
        System.out.println();
        System.out.println("--------------------");

        // Init Ignite.
        IgniteConfiguration iCfg = new IgniteConfiguration()
                .setBinaryConfiguration(
                        new BinaryConfiguration().setCompactFooter(false)
                )
                .setClientMode(false)
                .setDiscoverySpi(new TcpDiscoverySpi() {
                    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                        //No-op.
                    }
                });

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), iCfg, new NullLogger());

        MarshallerContextBenchImpl marshCtx = new MarshallerContextBenchImpl();

        marshCtx.onMarshallerProcessorStarted(new GridBenchKernalContext(new NullLogger(), iCfg), null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(marshCtx);

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        marshaller = marsh;
        binaryCtx = ctx;


        // Init MsgPack.
        msgPackMapper = new ObjectMapper(new MessagePackFactory(packerConfig));
        msgPackWriter = msgPackMapper.writerFor(IntPojo.class);
    }

    @Benchmark
    public byte[] writePojoMsgPack() throws Exception {
        // This uses TLS buffers and does not allocate on repeated calls.
        return msgPackWriter.writeValueAsBytes(new IntPojo(randomInt()));
    }

    @Benchmark
    public byte[] writePojoIgnite() throws Exception {
        // This uses TLS buffers and does not allocate on repeated calls.
        return marshaller.marshal(new IntPojo(randomInt()));
    }

    @Benchmark
    public byte[] writePrimitivesMsgPack() throws Exception {
        ByteArrayOutputStream s = new ByteArrayOutputStream();

        // TODO: This causes double buffering, which hurts perf.
        // Find a way to pack directly into a stream.
        msgPackMapper.writeValue(s, "Hello world");
        msgPackMapper.writeValue(s, 42);

        s.close();
        return s.toByteArray();
    }

    @Benchmark
    public byte[] writePrimitivesMsgPackRaw() throws Exception {
        PooledMessagePacker packer = new PooledMessagePacker(msgPackPooledOutput, packerConfig);

        packer
                .packString("Hello world")
                .packInt(42);

        packer.close();

        return packer.toByteArray();
    }

    //@Benchmark
    public byte[] writePrimitivesIgnite() {
        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(binaryCtx)) {
            writer.writeInt(randomInt());
            writer.writeString("Hello world");

            return writer.array();
        }
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        JmhBinaryMarshallerMsgPackBenchmark bench = new JmhBinaryMarshallerMsgPackBenchmark();
        bench.setup();

        printBytes(bench.writePrimitivesMsgPack());
        printBytes(bench.writePrimitivesMsgPackRaw());

        System.out.println();



//        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
//        ObjectWriter objectWriter = objectMapper.writerFor(IntPojo.class);
////
//        printBytes(objectWriter.writeValueAsBytes(new IntPojo(25)));
//



//        long t = System.currentTimeMillis();
//
//        while (System.currentTimeMillis() - t < 10000)
//        {
//            // objectWriter.writeValueAsBytes(new IntPojo((int) t));
//            // bench.writePrimitivesMsgPackRaw();
//            bench.writePrimitivesMsgPackRaw();
//        }



        JmhIdeBenchmarkRunner runner = JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(1)
                .benchmarks(JmhBinaryMarshallerMsgPackBenchmark.class.getSimpleName())
                .jvmArguments("-Xms4g", "-Xmx4g");

        runner
                .benchmarkModes(Mode.Throughput)
                .run();
    }

    private static void printBytes(byte[] res) {
        System.out.println();

        for (byte b : res)
            System.out.print(b + " ");

        System.out.println();
    }

}
