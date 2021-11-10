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

package org.apache.ignite.internal.client.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.UUID;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Message packer benchmark.
 *
 * <p>Benchmark                                 Mode  Cnt          Score          Error  Units
 *    ClientMessagePackerBenchmark.packInt     thrpt    3  389568322.545 ± 48247870.394  ops/s
 *    ClientMessagePackerBenchmark.packString  thrpt    3   30113891.585 ±  1781762.180  ops/s
 *    ClientMessagePackerBenchmark.packUuid    thrpt    3  331495906.854 ± 12134046.484  ops/s
 */
@State(Scope.Benchmark)
public class ClientMessagePackerBenchmark {
    private static final ByteBuf buffer = Unpooled.buffer(1024);
    
    private static final UUID uuid = UUID.randomUUID();
    
    /**
     * String benchmark.
     */
    @Benchmark
    public void packString() {
        var packer = new ClientMessagePacker(buffer.writerIndex(0));
        
        packer.packString("The quick brown fox jumps over the lazy dog.");
    }
    
    /**
     * Int benchmark.
     */
    @Benchmark
    public void packInt() {
        var packer = new ClientMessagePacker(buffer.writerIndex(0));
        
        packer.packInt(10);
        packer.packInt(Integer.MAX_VALUE);
    }
    
    /**
     * UUID benchmark.
     */
    @Benchmark
    public void packUuid() {
        var packer = new ClientMessagePacker(buffer.writerIndex(0));
        
        packer.packUuid(uuid);
    }
    
    /**
     * Runner.
     *
     * @param args Arguments.
     * @throws RunnerException Exception.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ClientMessagePackerBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(10))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(10))
                .forks(1)
                .build();
        
        new Runner(opt).run();
    }
}
