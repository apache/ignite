/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Tests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Ignite.Table;
    using NUnit.Framework;
    using Table;

    /// <summary>
    /// Base class for client tests.
    /// </summary>
    public class IgniteTestsBase
    {
        protected const string TableName = "PUB.tbl1";

        protected const string KeyCol = "key";

        protected const string ValCol = "val";

        private static readonly JavaServer ServerNode;

        private TestEventListener _eventListener = null!;

        static IgniteTestsBase()
        {
            ServerNode = JavaServer.StartAsync().GetAwaiter().GetResult();

            AppDomain.CurrentDomain.ProcessExit += (_, _) => ServerNode.Dispose();
        }

        protected static int ServerPort => ServerNode.Port;

        protected IIgniteClient Client { get; private set; } = null!;

        protected ITable Table { get; private set; } = null!;

        protected IRecordView<IIgniteTuple> TupleView { get; private set; } = null!;

        protected IRecordView<Poco> PocoView { get; private set; } = null!;

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            _eventListener = new TestEventListener();

            Client = await IgniteClient.StartAsync(GetConfig());

            Table = (await Client.Tables.GetTableAsync(TableName))!;
            TupleView = Table.RecordBinaryView;
            PocoView = Table.GetRecordView<Poco>();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            // ReSharper disable once ConstantConditionalAccessQualifier
            Client?.Dispose();

            Assert.Greater(_eventListener.BuffersRented, 0);
            Assert.AreEqual(_eventListener.BuffersReturned, _eventListener.BuffersRented);
            _eventListener.Dispose();
        }

        [TearDown]
        public async Task TearDown()
        {
            await TupleView.DeleteAllAsync(null, Enumerable.Range(-5, 20).Select(x => GetTuple(x)));

            Assert.AreEqual(_eventListener.BuffersReturned, _eventListener.BuffersRented);
        }

        protected static IIgniteTuple GetTuple(long id, string? val = null) =>
            new IgniteTuple { [KeyCol] = id, [ValCol] = val };

        protected static Poco GetPoco(long id, string? val = null) => new() {Key = id, Val = val};

        protected static IgniteClientConfiguration GetConfig() => new()
        {
            Endpoints = { "127.0.0.1:" + ServerNode.Port }
        };
    }
}
