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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;
import org.msgpack.jackson.dataformat.JsonArrayFormat;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.ImmutableValue;
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
 *
 * JmhBinaryMarshallerMsgPackBenchmark.writePojoIgnite            thrpt   10  11590924.790 ±  42061.734  ops/s
 * JmhBinaryMarshallerMsgPackBenchmark.writePojoMsgPack           thrpt   10   5386377.535 ±  33835.097  ops/s // Fields with names
 * JmhBinaryMarshallerMsgPackBenchmark.writePojoMsgPack2          thrpt   10   8505961.494 ± 465369.449  ops/s // Fields without names (array)
 *
 * JmhBinaryMarshallerMsgPackBenchmark.readPrimitivesIgnite       thrpt   10  19873521.096 ± 545779.558  ops/s
 * JmhBinaryMarshallerMsgPackBenchmark.readPrimitivesMsgPack      thrpt   10  29235107.372 ±  85371.004  ops/s
 *
 * JmhBinaryMarshallerMsgPackBenchmark.readPojoIgnite             thrpt   10  8437054.066 ± 104476.415  ops/s
 * JmhBinaryMarshallerMsgPackBenchmark.readPojoMsgPack            thrpt   10  6292876.474 ±  73356.915  ops/s
 * JmhBinaryMarshallerMsgPackBenchmark.readPojoMsgPackBinary      thrpt   10  19223554.149 ± 114627.713  ops/s
 *
 */
@State(Scope.Benchmark)
public class JmhBinaryMarshallerMsgPackBenchmark extends JmhAbstractBenchmark {
    private static final MessagePack.PackerConfig packerConfig = new MessagePack.PackerConfig().withBufferSize(128);

    private static final PooledMessageBufferOutput msgPackPooledOutput = new PooledMessageBufferOutput();

    private static final byte[] ignitePrimitiveBytes = new byte[]{42, 0, 0, 0, 9, 11, 0, 0, 0, 72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100};
    private static final byte[] msgPackPrimitiveBytes = new byte[]{42, -85, 72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100};

    private static final byte[] ignitePojoBytes = new byte[]{103, 1, 11, 0, 4, -118, -2, 77, -72, 54, -14, 1, 34, 0, 0, 0, 48, -14, 1, 111, 29, 0, 0, 0, 3, 42, 0, 0, 0, 33, -57, 1, 0, 24};
    private static final byte[] msgPackPojoBytes = new byte[]{-127, -93, 118, 97, 108, 42};

    private BinaryMarshaller marshaller;
    private BinaryContext binaryCtx;

    private ObjectMapper msgPackMapper;
    private ObjectWriter msgPackWriter;

    private ObjectMapper msgPackMapper2;
    private ObjectWriter msgPackWriter2;

    private MessageUnpacker msgPackUnpacker;

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
        writePojoIgnite(); // Cache meta for read.


        // Init MsgPack.
        msgPackMapper = new ObjectMapper(new MessagePackFactory(packerConfig));
        msgPackWriter = msgPackMapper.writerFor(IntPojo.class);

        // Init MsgPack without field names.
        msgPackMapper2 = new ObjectMapper(new MessagePackFactory(packerConfig)).setAnnotationIntrospector(new JsonArrayFormat());
        msgPackWriter2 = msgPackMapper2.writerFor(IntPojo.class);

        msgPackUnpacker = MessagePack.newDefaultUnpacker(msgPackPrimitiveBytes);
    }

    //@Benchmark
    public byte[] writePojoMsgPack() throws Exception {
        // This uses TLS buffers and does not allocate on repeated calls.
        return msgPackWriter.writeValueAsBytes(new IntPojo(42));
    }

    //@Benchmark
    public byte[] writePojoMsgPack2() throws Exception {
        // This uses TLS buffers and does not allocate on repeated calls.
        return msgPackWriter2.writeValueAsBytes(new IntPojo(42));
    }

    //@Benchmark
    public byte[] writePojoIgnite() throws Exception {
        // This uses TLS buffers and does not allocate on repeated calls.
        return marshaller.marshal(new IntPojo(42));
    }

    //@Benchmark
    public byte[] writePrimitivesMsgPack() throws Exception {
        ByteArrayOutputStream s = new ByteArrayOutputStream();

        // TODO: This causes double buffering, which hurts perf.
        // Find a way to pack directly into a stream.
        msgPackMapper.writeValue(s, 42);
        msgPackMapper.writeValue(s, "Hello world");

        s.close();
        return s.toByteArray();
    }

    //@Benchmark
    public byte[] writePrimitivesMsgPackRaw() throws Exception {
        PooledMessagePacker packer = new PooledMessagePacker(msgPackPooledOutput, packerConfig);

        packer
                .packInt(42)
                .packString("Hello world");

        packer.close();

        return packer.toByteArray();
    }

    // @Benchmark
    public byte[] writePrimitivesIgnite() {
        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(binaryCtx)) {
            writer.writeInt(42);
            writer.writeString("Hello world");

            return writer.array();
        }
    }

    // @Benchmark
    public IgniteBiTuple<Integer, String> readPrimitivesIgnite() throws Exception {
        try (BinaryReaderExImpl reader = new BinaryReaderExImpl(binaryCtx, new BinaryHeapInputStream(ignitePrimitiveBytes), null, true)) {
            return new IgniteBiTuple<>(reader.readInt(), reader.readString());
        }
    }

    // @Benchmark
    public IgniteBiTuple<Integer, String> readPrimitivesMsgPack() throws Exception {
        msgPackUnpacker.reset(new ArrayBufferInput(msgPackPrimitiveBytes));

        return new IgniteBiTuple<>(msgPackUnpacker.unpackInt(), msgPackUnpacker.unpackString());
    }

    @Benchmark
    public IntPojo readPojoIgnite() throws Exception {
        return marshaller.unmarshal(ignitePojoBytes, null);
    }

    @Benchmark
    public IntPojo readPojoMsgPack() throws Exception {
        return msgPackMapper.readValue(msgPackPojoBytes, IntPojo.class);
    }

    @Benchmark
    public ImmutableValue readPojoMsgPackBinary() throws Exception {
        msgPackUnpacker.reset(new ArrayBufferInput(msgPackPojoBytes));

        return msgPackUnpacker.unpackValue();
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

        System.out.println(bench.readPojoMsgPackBinary());

        printBytes(bench.writePrimitivesMsgPackRaw());
        printBytes(bench.writePrimitivesIgnite());
        printBytes(bench.writePojoMsgPack());
        printBytes(bench.writePojoIgnite());

        System.out.println();



//        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
//        ObjectWriter objectWriter = objectMapper.writerFor(IntPojo.class);
////
//        printBytes(objectWriter.writeValueAsBytes(new IntPojo(25)));
//



//        long t = System.currentTimeMillis();
//
//        while (System.currentTimeMillis() - t < 20000)
//        {
//            bench.readPojoMsgPack();
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
            System.out.print(b + ", ");

        System.out.println();
    }

}
