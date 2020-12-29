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

namespace Apache.Ignite.BenchmarkDotNet.Binary
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Memory;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// System type reading benchmark. Checks <see cref="BinarySystemHandlers.TryReadSystemType{T}"/> performance.
    /// </summary>
    public class BinarySystemTypeReadBenchmark
    {
        /** Marshaller. */
        private readonly Marshaller _marsh = new Marshaller(new BinaryConfiguration());

        /** Memory manager. */
        private readonly PlatformMemoryManager _memMgr = new PlatformMemoryManager(1024);

        /** Memory chunk. */
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

            // TODO: All system types.
            writer.Write((byte)1);
            writer.Write((short)2);
            writer.Write((int)3);
            writer.Write((long)4);
            writer.Write((float)5.5);
            writer.Write((double)6.6);
            writer.Write((decimal)7.7);

            stream.SynchronizeOutput();
        }

        [Benchmark]
        public void Read()
        {
            var stream = _mem.GetStream();
            var reader = _marsh.StartUnmarshal(stream);

            Assert(1, reader.ReadObject<byte>());
            Assert(2, reader.ReadObject<short>());
            Assert(3, reader.ReadObject<int>());
            Assert(4, reader.ReadObject<long>());
            Assert(5.5f, reader.ReadObject<float>());
            Assert(6.6d, reader.ReadObject<double>());
            Assert(7.7m, reader.ReadObject<decimal>());
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
