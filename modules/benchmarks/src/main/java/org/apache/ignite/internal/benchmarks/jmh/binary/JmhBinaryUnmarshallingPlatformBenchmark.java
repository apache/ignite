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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.GridKernalGatewayImpl;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.platform.memory.PlatformBigEndianInputStreamImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformBigEndianOutputStreamImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStreamImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStreamImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.systemview.jmx.JmxSystemViewExporterSpi;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark umarshalling in Platform (detach vs not detach).
 */
public class JmhBinaryUnmarshallingPlatformBenchmark extends JmhAbstractBenchmark {
    /** */
    @State(Scope.Thread)
    public static class Context {
        /** */
        final PlatformMemory mem = new MockPlatformMemory();

        /** */
        BinaryMarshaller marsh;

        /** size of tested collection. */
        @Param({"10", "50", "100", "500", "1000", "5000", "10000", "20000", "50000", "100000"})
        int size;

        /**  Size of string parameter of value. */
        @Param({"0", "10", "30", "50"})
        int strSize;

        /** */
        @Setup
        public void setup() throws Exception {
            marsh = binaryMarshaller();

            Collection<Value> col = IntStream.range(0, size).mapToObj(i ->
                    new Value(i, strSize > 0 ? randomString(strSize) : null)).collect(Collectors.toList());

            marshall(col);
        }

        /** */
        @TearDown
        public void tearDown() {
            mem.close();
        }

        /** */
        private void marshall(Object val) {
            BinaryWriterExImpl writer = marsh.binaryMarshaller().writer(mem.output());

            writer.writeObject(val);
        }
    }

    /**
     * Test unmarshalling detached object.
     */
    @Benchmark
    public void testUnmarshallingDetach(Context ctx, Blackhole bh) {
        BinaryRawReaderEx reader = ctx.marsh.binaryMarshaller().reader(ctx.mem.input());

        Object out = reader.readObjectDetached();

        assert out != null;

        bh.consume(out);
    }

    /**
     * Test unmarshalling not detached object.
     */
    @Benchmark
    public void testUnmarshalling(Context ctx, Blackhole bh) {
        BinaryReaderExImpl reader = ctx.marsh.binaryMarshaller().reader(ctx.mem.input());

        Object out = reader.unmarshal(0);

        assert out != null;

        bh.consume(out);
    }

    /**
     * Configure {@link BinaryMarshaller} for test.
     */
    private static BinaryMarshaller binaryMarshaller() throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        iCfg.setBinaryConfiguration(new BinaryConfiguration());
        iCfg.setClientMode(false);
        iCfg.setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                //No-op.
            }
        });
        iCfg.setSystemViewExporterSpi(new JmxSystemViewExporterSpi());
        iCfg.setGridLogger(new NullLogger());

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), iCfg, new NullLogger());

        GridKernalContextImpl kernCtx = new MockKernalContext(iCfg);
        kernCtx.add(new GridSystemViewManager(kernCtx));
        kernCtx.add(new GridDiscoveryManager(kernCtx));

        MarshallerContextImpl marshCtx = new MockMarshallerContext();
        marshCtx.onMarshallerProcessorStarted(kernCtx, null);

        BinaryMarshaller marsh = new BinaryMarshaller();
        marsh.setContext(marshCtx);
        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
    }

    /** */
    private static class MockKernalContext extends GridKernalContextImpl {
        /** */
        public MockKernalContext() {
            // No-op.
        }

        /** */
        public MockKernalContext(IgniteConfiguration cfg) {
            this(cfg, new NullLogger());
        }

        /** */
        public MockKernalContext(IgniteConfiguration cfg, IgniteLogger log) {
            super(new GridLoggerProxy(log, null, null, null), new IgniteKernal(null), cfg,
                new GridKernalGatewayImpl(null), null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, U.allPluginProviders(), null, null, null,
                new LongJVMPauseDetector(log));
        }
    }

    /** */
    private static class MockMarshallerContext extends MarshallerContextImpl {
        /** */
        private static final ConcurrentMap<Integer, String> clsMap = new ConcurrentHashMap<>();

        /** */
        public MockMarshallerContext() {
            super(null, null);
        }

        /** {@inheritDoc} */
        @Override public boolean registerClassName(
                byte platformId,
                int typeId,
                String clsName,
                boolean failIfUnregistered
        ) throws IgniteCheckedException {
            String oldClsName = clsMap.putIfAbsent(typeId, clsName);

            if (oldClsName != null && !oldClsName.equals(clsName))
                throw new IgniteCheckedException("Duplicate ID [id=" + typeId + ", oldClsName=" + oldClsName +
                        ", clsName=" + clsName + ']');

            return true;
        }

        /** {@inheritDoc} */
        @Override public String getClassName(byte platformId, int typeId) {
            return clsMap.get(typeId);
        }
    }

    /** */
    private static class MockPlatformMemory implements PlatformMemory {
        /** */
        private static final int DEFAULT_CAPACITY = 1024;

        /** */
        private long memPtr;

        /** */
        private int capacity;

        /** */
        public MockPlatformMemory() {
            this(DEFAULT_CAPACITY);
        }

        /**
         * @param cap Initial capacity of underlying buffer.
         */
        public MockPlatformMemory(int cap) {
            this(GridUnsafe.allocateMemory(cap), cap);
        }

        /**
         * @param ptr Pointer to memory chunk.
         * @param cap Initial capacity of underlying buffer.
         */
        public MockPlatformMemory(long ptr, int cap) {
            memPtr = ptr;
            capacity = cap;
        }

        /** {@inheritDoc} */
        @Override public PlatformInputStream input() {
            return GridUnsafe.BIG_ENDIAN ? new PlatformBigEndianInputStreamImpl(this) :
                    new PlatformInputStreamImpl(this);
        }

        /** {@inheritDoc} */
        @Override public PlatformOutputStream output() {
            return GridUnsafe.BIG_ENDIAN ? new PlatformBigEndianOutputStreamImpl(this) :
                    new PlatformOutputStreamImpl(this);
        }

        /** {@inheritDoc} */
        @Override public long pointer() {
            return memPtr;
        }

        /** {@inheritDoc} */
        @Override public long data() {
            return memPtr;
        }

        /** {@inheritDoc} */
        @Override public int capacity() {
            return capacity;
        }

        /** {@inheritDoc} */
        @Override public int length() {
            return capacity;
        }

        /** {@inheritDoc} */
        @Override public void reallocate(int cap) {
            memPtr = GridUnsafe.reallocateMemory(memPtr, cap);

            capacity = cap;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            GridUnsafe.freeMemory(memPtr);
        }
    }

    /** */
    private static class Value {
        /** */
        final int id;

        /** */
        final String name;

        /** */
        Value(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JmhBinaryUnmarshallingPlatformBenchmark.class.getSimpleName())
                .threads(1)
                .forks(1)
                .jvmArgs("-Xmx1G", "-Xms1G")
                .warmupIterations(10)
                .resultFormat(ResultFormatType.CSV)
                .measurementIterations(20)
                .build();

        new Runner(opt).run();
    }
}
