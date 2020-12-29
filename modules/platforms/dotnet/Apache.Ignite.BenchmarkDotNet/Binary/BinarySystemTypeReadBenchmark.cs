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

// ReSharper disable RedundantCast
namespace Apache.Ignite.BenchmarkDotNet.Binary
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Memory;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// System type reading benchmark. Checks <see cref="BinarySystemHandlers.TryReadSystemType{T}"/> performance.
    /// <para />
    /// | Method |     Mean |    Error |   StdDev |  Gen 0 | Gen 1 | Gen 2 | Allocated |
    /// |------- |---------:|---------:|---------:|-------:|------:|------:|----------:|
    /// |   Read | 22.10 us | 0.107 us | 0.100 us | 1.5869 |     - |     - |   9.79 KB |
    /// </summary>
    [MemoryDiagnoser]
    public class BinarySystemTypeReadBenchmark
    {
        /** */
        private static readonly DateTime DateTime = new DateTime(2010, 10, 10).ToUniversalTime();

        /** */
        private static readonly Guid Guid = Guid.NewGuid();

        /** */
        private readonly Marshaller _marsh = new Marshaller(new BinaryConfiguration {ForceTimestamp = true});

        /** */
        private readonly PlatformMemoryManager _memMgr = new PlatformMemoryManager(1024);

        /** */
        private IPlatformMemory _mem;

        /// <summary>
        /// Sets up the benchmark.
        /// </summary>
        [GlobalSetup]
        public void Setup()
        {
            _mem = _memMgr.Allocate();
            var stream = _mem.GetStream();
            var writer = _marsh.StartMarshal(stream);

            writer.Write(true);
            writer.Write('i');
            writer.Write((byte) 1);
            writer.Write((short) 2);
            writer.Write((int) 3);
            writer.Write((long) 4);
            writer.Write((float) 5.5);
            writer.Write((double) 6.6);
            writer.Write((decimal) 7.7);
            writer.Write(DateTime);
            writer.Write(Guid);

            writer.Write(new[] {true});
            writer.Write(new[] {'i'});
            writer.Write(new[] {(byte) 1});
            writer.Write(new[] {(short) 2});
            writer.Write(new[] {(int) 3});
            writer.Write(new[] {(long) 4});
            writer.Write(new[] {(float) 5.5});
            writer.Write(new[] {(double) 6.6});
            writer.Write(new[] {(decimal?) 7.7});
            writer.Write(new DateTime?[] {DateTime});
            writer.Write(new Guid?[] {Guid});

            stream.SynchronizeOutput();
        }

        [Benchmark]
        public void Read()
        {
            var stream = _mem.GetStream();
            var reader = _marsh.StartUnmarshal(stream);

            Assert(true, reader.ReadObject<bool>());
            Assert('i', reader.ReadObject<char>());
            Assert(1, reader.ReadObject<byte>());
            Assert(2, reader.ReadObject<short>());
            Assert(3, reader.ReadObject<int>());
            Assert(4, reader.ReadObject<long>());
            Assert(5.5f, reader.ReadObject<float>());
            Assert(6.6d, reader.ReadObject<double>());
            Assert(7.7m, reader.ReadObject<decimal>());
            Assert(DateTime, reader.ReadObject<DateTime>());
            Assert(Guid, reader.ReadObject<Guid>());

            Assert(true, reader.ReadObject<bool[]>()[0]);
            Assert('i', reader.ReadObject<char[]>()[0]);
            Assert(1, reader.ReadObject<byte[]>()[0]);
            Assert(2, reader.ReadObject<short[]>()[0]);
            Assert(3, reader.ReadObject<int[]>()[0]);
            Assert(4, reader.ReadObject<long[]>()[0]);
            Assert(5.5f, reader.ReadObject<float[]>()[0]);
            Assert(6.6d, reader.ReadObject<double[]>()[0]);
            Assert(7.7m, reader.ReadObject<decimal?[]>()[0]);
            Assert(DateTime, reader.ReadObject<DateTime?[]>()[0]);
            Assert(Guid, reader.ReadObject<Guid?[]>()[0]);
        }

        // ReSharper disable ParameterOnlyUsedForPreconditionCheck.Local
        private static void Assert<T>(T expected, T actual)
        {
            if (!Equals(expected, actual))
            {
                throw new Exception("Unexpected value");
            }
        }
    }
}
