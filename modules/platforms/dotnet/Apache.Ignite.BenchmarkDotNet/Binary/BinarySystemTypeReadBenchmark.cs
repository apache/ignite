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
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Memory;
    using global::BenchmarkDotNet.Attributes;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;

    /// <summary>
    /// System type reading benchmark. Checks <see cref="BinarySystemHandlers.TryReadSystemType{T}"/> performance.
    /// <para />
    /// Results on Core i7-9700K, Ubuntu 20.04, .NET Core 3.1:
    /// | Method |     Mean |   Error |  StdDev |  Gen 0 | Gen 1 | Gen 2 | Allocated |
    /// |------- |---------:|--------:|--------:|-------:|------:|------:|----------:|
    /// |   Read | 849.4 ns | 4.44 ns | 4.16 ns | 0.0725 |     - |     - |     456 B |
    /// </summary>
    [MemoryDiagnoser]
    public class BinarySystemTypeReadBenchmark
    {
        /** */
        private static readonly DateTime DateTime = new DateTime(2010, 10, 10).ToUniversalTime();

        /** */
        private static readonly Guid Guid = Guid.NewGuid();

        /** */
        private static readonly Marshaller Marsh = new Marshaller(new BinaryConfiguration {ForceTimestamp = true});

        /** */
        private static readonly PlatformMemoryManager MemMgr = new PlatformMemoryManager(1024);

        /** */
        private BinaryReader _reader;

        /// <summary>
        /// Sets up the benchmark.
        /// </summary>
        [GlobalSetup]
        public void Setup()
        {
            var mem = MemMgr.Allocate();
            var stream = mem.GetStream();
            var writer = Marsh.StartMarshal(stream);

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

            _reader = Marsh.StartUnmarshal(stream);
        }

        [Benchmark]
        public void Read()
        {
            _reader.Stream.Seek(0, SeekOrigin.Begin);

            _reader.ReadObject<bool>();
            _reader.ReadObject<char>();
            _reader.ReadObject<byte>();
            _reader.ReadObject<short>();
            _reader.ReadObject<int>();
            _reader.ReadObject<long>();
            _reader.ReadObject<float>();
            _reader.ReadObject<double>();
            _reader.ReadObject<decimal>();
            _reader.ReadObject<DateTime>();
            _reader.ReadObject<Guid>();

            _reader.ReadObject<bool[]>();
            _reader.ReadObject<char[]>();
            _reader.ReadObject<byte[]>();
            _reader.ReadObject<short[]>();
            _reader.ReadObject<int[]>();
            _reader.ReadObject<long[]>();
            _reader.ReadObject<float[]>();
            _reader.ReadObject<double[]>();
            _reader.ReadObject<decimal?[]>();
            _reader.ReadObject<DateTime?[]>();
            _reader.ReadObject<Guid?[]>();
        }
    }
}
