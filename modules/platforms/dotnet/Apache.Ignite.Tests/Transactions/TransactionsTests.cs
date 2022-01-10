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

namespace Apache.Ignite.Tests.Transactions
{
    using System.Linq;
    using System.Threading.Tasks;
    using System.Transactions;
    using Ignite.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ITransactions"/> and <see cref="ITransaction"/>.
    /// </summary>
    public class TransactionsTests : IgniteTestsBase
    {
        [Test]
        public async Task TestRecordViewBinaryOperations()
        {
            var key = GetTuple(1);
            await Table.UpsertAsync(null, GetTuple(1, "1"));

            await using var tx = await Client.Transactions.BeginAsync();
            await Table.UpsertAsync(tx, GetTuple(1, "22"));

            Assert.IsFalse(await Table.DeleteExactAsync(tx, GetTuple(1, "1")));

            Assert.IsFalse(await Table.InsertAsync(tx, GetTuple(1, "111")));
            Assert.AreEqual(GetTuple(1, "22"), await Table.GetAsync(tx, key));
            Assert.AreEqual(GetTuple(1, "22"), await Table.GetAndUpsertAsync(tx, GetTuple(1, "33")));
            Assert.AreEqual(GetTuple(1, "33"), await Table.GetAndReplaceAsync(tx, GetTuple(1, "44")));
            Assert.IsTrue(await Table.ReplaceAsync(tx, GetTuple(1, "55")));
            Assert.AreEqual(GetTuple(1, "55"), await Table.GetAndDeleteAsync(tx, key));
            Assert.IsFalse(await Table.DeleteAsync(tx, key));

            await Table.UpsertAllAsync(tx, new[] { GetTuple(1, "6"), GetTuple(2, "7") });
            Assert.AreEqual(2, (await Table.GetAllAsync(tx, new[] { key, GetTuple(2), GetTuple(3) })).Count);

            var insertAllRes = await Table.InsertAllAsync(tx, new[] { GetTuple(1, "8"), GetTuple(3, "9") });
            Assert.AreEqual(GetTuple(1, "6"), await Table.GetAsync(tx, key));
            Assert.AreEqual(GetTuple(1, "8"), insertAllRes.Single());

            Assert.IsFalse(await Table.ReplaceAsync(tx, GetTuple(-1)));
            Assert.IsTrue(await Table.ReplaceAsync(tx, GetTuple(1, "10")));
            Assert.AreEqual(GetTuple(1, "10"), await Table.GetAsync(tx, key));

            Assert.IsFalse(await Table.ReplaceAsync(tx, GetTuple(1, "1"), GetTuple(1, "11")));
            Assert.IsTrue(await Table.ReplaceAsync(tx, GetTuple(1, "10"), GetTuple(1, "12")));
            Assert.AreEqual(GetTuple(1, "12"), await Table.GetAsync(tx, key));

            var deleteAllRes = await Table.DeleteAllAsync(tx, new[] { GetTuple(3), GetTuple(4) });
            Assert.AreEqual(4, deleteAllRes.Single()[0]);
            Assert.IsNull(await Table.GetAsync(tx, GetTuple(3)));

            var deleteAllExactRes = await Table.DeleteAllAsync(tx, new[] { GetTuple(1, "12"), GetTuple(5) });
            Assert.AreEqual(5, deleteAllExactRes.Single()[0]);
            Assert.IsNull(await Table.GetAsync(tx, key));

            await tx.RollbackAsync();
            Assert.AreEqual(GetTuple(1, "1"), await Table.GetAsync(null, key));
        }

        [Test]
        public async Task TestCommitUpdatesData()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await Table.UpsertAsync(tx, GetTuple(1, "2"));
            await tx.CommitAsync();

            var res = await Table.GetAsync(null, GetTuple(1));
            Assert.AreEqual("2", res![ValCol]);
        }

        [Test]
        public async Task TestRollbackDoesNotUpdateData()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await Table.UpsertAsync(tx, GetTuple(1, "2"));
            await tx.RollbackAsync();

            var res = await Table.GetAsync(null, GetTuple(1));
            Assert.IsNull(res);
        }

        [Test]
        public async Task TestDisposeDoesNotUpdateData()
        {
            await using (var tx = await Client.Transactions.BeginAsync())
            {
                await Table.UpsertAsync(tx, GetTuple(1, "2"));
                await tx.RollbackAsync();
            }

            var res = await Table.GetAsync(null, GetTuple(1));
            Assert.IsNull(res);
        }

        [Test]
        public async Task TestCommitRollbackSameTxThrows()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await tx.CommitAsync();

            var ex = Assert.ThrowsAsync<TransactionException>(async () => await tx.RollbackAsync());
            Assert.AreEqual("Transaction is already committed.", ex?.Message);
        }

        [Test]
        public async Task TestRollbackCommitSameTxThrows()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await tx.RollbackAsync();

            var ex = Assert.ThrowsAsync<TransactionException>(async () => await tx.CommitAsync());
            Assert.AreEqual("Transaction is already rolled back.", ex?.Message);
        }

        [Test]
        public async Task TestMultipleDisposeIsAllowed()
        {
            var tx = await Client.Transactions.BeginAsync();

            await tx.DisposeAsync();
            await tx.DisposeAsync();

            var ex = Assert.ThrowsAsync<TransactionException>(async () => await tx.CommitAsync());
            Assert.AreEqual("Transaction is already rolled back.", ex?.Message);
        }

        [Test]
        public void TestCustomTransactionInterfaceThrows()
        {
            var ex = Assert.ThrowsAsync<TransactionException>(
                async () => await Table.UpsertAsync(new CustomTx(), GetTuple(1, "2")));

            StringAssert.StartsWith("Unsupported transaction implementation", ex?.Message);
        }

        [Test]
        public async Task TestClientDisconnectClosesActiveTransactions()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));

            using (var client2 = await IgniteClient.StartAsync(GetConfig()))
            {
                var table = await client2.Tables.GetTableAsync(TableName);
                var tx = await client2.Transactions.BeginAsync();

                await table!.RecordView.UpsertAsync(tx, GetTuple(1, "2"));
            }

            Assert.AreEqual("1", (await Table.GetAsync(null, GetTuple(1)))![ValCol]);
        }

        [Test]
        public async Task TestTransactionFromAnotherClientThrows()
        {
            using var client2 = await IgniteClient.StartAsync(GetConfig());
            await using var tx = await client2.Transactions.BeginAsync();

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await Table.UpsertAsync(tx, GetTuple(1, "2")));
            Assert.AreEqual("Specified transaction belongs to a different IgniteClient instance.", ex!.Message);
        }

        private class CustomTx : ITransaction
        {
            public ValueTask DisposeAsync()
            {
                return new ValueTask(Task.CompletedTask);
            }

            public Task CommitAsync()
            {
                return Task.CompletedTask;
            }

            public Task RollbackAsync()
            {
                return Task.CompletedTask;
            }
        }
    }
}
