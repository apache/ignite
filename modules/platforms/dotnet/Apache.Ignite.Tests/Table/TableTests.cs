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

namespace Apache.Ignite.Tests.Table
{
    using System.Threading.Tasks;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ITable"/>.
    /// </summary>
    public class TableTests : IgniteTestsBase
    {
        [Test]
        public async Task TestUpsertGet()
        {
            using var client = await IgniteClient.StartAsync(GetConfig());
            var table = (await client.Tables.GetTableAsync("PUB.tbl1"))!;

            var tuple = new IgniteTuple
            {
                ["key"] = 1,
                ["val"] = "foo"
            };

            await table.UpsertAsync(tuple);

            var keyTuple = new IgniteTuple
            {
                ["key"] = 1
            };

            var resTuple = (await table.GetAsync(keyTuple))!;

            Assert.IsNotNull(resTuple);
            Assert.AreEqual(2, resTuple.FieldCount);
            Assert.AreEqual(1L, resTuple["key"]);
            Assert.AreEqual("foo", resTuple["val"]);
        }
    }
}
