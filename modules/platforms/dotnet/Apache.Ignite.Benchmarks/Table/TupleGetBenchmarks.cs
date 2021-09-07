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

namespace Apache.Ignite.Benchmarks.Table
{
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using Ignite.Table;
    using Tests;

    /// <summary>
    /// Results on Intel Core i7-7700HQ, .NET SDK 5.0.400, Ubuntu 20.04:
    /// | Method |     Mean |   Error |  StdDev |
    /// |------- |---------:|--------:|--------:|
    /// |    Get | 202.0 us | 3.71 us | 7.82 us |.
    /// </summary>
    public class TupleGetBenchmarks
    {
        private JavaServer? _javaServer;
        private IIgniteClient? _client;
        private ITable _table = null!;
        private IgniteTuple _keyTuple = null!;

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _javaServer = await JavaServer.StartAsync();
            _client = await IgniteClient.StartAsync(new IgniteClientConfiguration("127.0.0.1:" + _javaServer.Port));
            _table = (await _client.Tables.GetTableAsync("PUB.tbl1"))!;

            var tuple = new IgniteTuple
            {
                ["key"] = 1,
                ["val"] = "foo"
            };

            await _table.UpsertAsync(tuple);

            _keyTuple = new IgniteTuple
            {
                ["key"] = 1
            };
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _client?.Dispose();
            _javaServer?.Dispose();
        }

        [Benchmark]
        public async Task Get()
        {
            await _table.GetAsync(_keyTuple);
        }
    }
}
