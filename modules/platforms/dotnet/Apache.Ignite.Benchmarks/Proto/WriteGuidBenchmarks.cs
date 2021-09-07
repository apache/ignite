// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Benchmarks.Proto
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using BenchmarkDotNet.Attributes;
    using Internal.Buffers;
    using Internal.Proto;

    /// <summary>
    /// Results on Intel Core i7-7700HQ, .NET SDK 5.0.400, Ubuntu 20.04:
    /// |        Method |     Mean |   Error |  StdDev |
    /// |-------------- |---------:|--------:|--------:|
    /// | WriteTwoLongs | 173.9 ns | 1.43 ns | 1.34 ns |
    /// |     WriteGuid | 171.1 ns | 3.38 ns | 3.16 ns |.
    /// </summary>
    [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Benchmarks.")]
    public class WriteGuidBenchmarks
    {
        private static readonly Guid Guid = Guid.NewGuid();

        [Benchmark]
        public void WriteTwoLongs()
        {
            using var bufferWriter = new PooledArrayBufferWriter();
            var writer = bufferWriter.GetMessageWriter();
            writer.Write(long.MaxValue);
            writer.Write(long.MaxValue);
            writer.Flush();
        }

        [Benchmark]
        public void WriteGuid()
        {
            using var bufferWriter = new PooledArrayBufferWriter();
            var writer = bufferWriter.GetMessageWriter();
            writer.Write(Guid);
            writer.Flush();
        }
    }
}
