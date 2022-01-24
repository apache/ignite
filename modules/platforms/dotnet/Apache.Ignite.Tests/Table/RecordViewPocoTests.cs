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
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests for POCO view.
    /// </summary>
    public class RecordViewPocoTests : IgniteTestsBase
    {
        [Test]
        public async Task TestUpsertGet()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "foo"));

            var keyTuple = GetPoco(1);
            var resTuple = (await PocoView.GetAsync(null, keyTuple))!;

            Assert.IsNotNull(resTuple);

            Assert.AreEqual(1L, resTuple.Key);
            Assert.AreEqual("foo", resTuple.Val);

            Assert.IsNull(resTuple.UnmappedStr);
            Assert.AreEqual(default(Guid), resTuple.UnmappedId);
        }

        [Test]
        public async Task TestUpsertOverridesPreviousValue()
        {
            var key = GetPoco(1);

            await PocoView.UpsertAsync(null, GetPoco(1, "foo"));
            Assert.AreEqual("foo", (await PocoView.GetAsync(null, key))!.Val);

            await PocoView.UpsertAsync(null, GetPoco(1, "bar"));
            Assert.AreEqual("bar", (await PocoView.GetAsync(null, key))!.Val);
        }

        [Test]
        public void TestUpsertEmptyPocoThrowsException()
        {
            var pocoView = Table.GetRecordView<object>();

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await pocoView.UpsertAsync(null, new object()));

            Assert.AreEqual("Missed key column: KEY", ex!.Message);
        }

        [Test]
        public async Task TestGetAndUpsertNonExistentRecordReturnsNull()
        {
            Poco? res = await PocoView.GetAndUpsertAsync(null, GetPoco(2, "2"));

            Assert.IsNull(res);
            Assert.AreEqual("2", (await PocoView.GetAsync(null, GetPoco(2)))!.Val);
        }

        [Test]
        public async Task TestGetAndUpsertExistingRecordOverwritesAndReturns()
        {
            await PocoView.UpsertAsync(null, GetPoco(2, "2"));
            Poco? res = await PocoView.GetAndUpsertAsync(null, GetPoco(2, "22"));

            Assert.IsNotNull(res);
            Assert.AreEqual(2, res!.Key);
            Assert.AreEqual("2", res.Val);
            Assert.AreEqual("22", (await PocoView.GetAsync(null, GetPoco(2)))!.Val);
        }

        [Test]
        public async Task TestGetAndDeleteNonExistentRecordReturnsNull()
        {
            Poco? res = await PocoView.GetAndDeleteAsync(null, GetPoco(2, "2"));

            Assert.IsNull(res);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(2)));
        }

        [Test]
        public async Task TestGetAndDeleteExistingRecordRemovesAndReturns()
        {
            await PocoView.UpsertAsync(null, GetPoco(2, "2"));
            Poco? res = await PocoView.GetAndDeleteAsync(null, GetPoco(2));

            Assert.IsNotNull(res);
            Assert.AreEqual(2, res!.Key);
            Assert.AreEqual("2", res.Val);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(2)));
        }

        [Test]
        public async Task TestInsertNonExistentKeyCreatesRecordReturnsTrue()
        {
            var res = await PocoView.InsertAsync(null, GetPoco(1, "1"));

            Assert.IsTrue(res);
            Assert.IsTrue(await PocoView.GetAsync(null, GetPoco(1)) != null);
        }

        [Test]
        public async Task TestInsertExistingKeyDoesNotOverwriteReturnsFalse()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            var res = await PocoView.InsertAsync(null, GetPoco(1, "2"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await PocoView.GetAsync(null, GetPoco(1)))!.Val);
        }

        [Test]
        public async Task TestDeleteNonExistentRecordReturnFalse()
        {
            Assert.IsFalse(await PocoView.DeleteAsync(null, GetPoco(-1)));
        }

        [Test]
        public async Task TestDeleteExistingRecordReturnsTrue()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));

            Assert.IsTrue(await PocoView.DeleteAsync(null, GetPoco(1)));
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
        }

        [Test]
        public async Task TestDeleteExactNonExistentRecordReturnsFalse()
        {
            Assert.IsFalse(await PocoView.DeleteExactAsync(null, GetPoco(-1)));
        }

        [Test]
        public async Task TestDeleteExactExistingKeyDifferentValueReturnsFalseDoesNotDelete()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));

            Assert.IsFalse(await PocoView.DeleteExactAsync(null, GetPoco(1)));
            Assert.IsFalse(await PocoView.DeleteExactAsync(null, GetPoco(1, "2")));
            Assert.IsNotNull(await PocoView.GetAsync(null, GetPoco(1)));
        }

        [Test]
        public async Task TestDeleteExactSameKeyAndValueReturnsTrueDeletesRecord()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));

            Assert.IsTrue(await PocoView.DeleteExactAsync(null, GetPoco(1, "1")));
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
        }

        [Test]
        public async Task TestReplaceNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "1"));

            Assert.IsFalse(res);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
        }

        [Test]
        public async Task TestReplaceExistingRecordReturnsTrueOverwrites()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "2"));

            Assert.IsTrue(res);
            Assert.AreEqual("2", (await PocoView.GetAsync(null, GetPoco(1)))!.Val);
        }

        [Test]
        public async Task TestGetAndReplaceNonExistentRecordReturnsNullDoesNotCreateRecord()
        {
            Poco? res = await PocoView.GetAndReplaceAsync(null, GetPoco(1, "1"));

            Assert.IsNull(res);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
        }

        [Test]
        public async Task TestGetAndReplaceExistingRecordReturnsOldOverwrites()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            Poco? res = await PocoView.GetAndReplaceAsync(null, GetPoco(1, "2"));

            Assert.IsNotNull(res);
            Assert.AreEqual("1", res!.Val);
            Assert.AreEqual("2", (await PocoView.GetAsync(null, GetPoco(1)))!.Val);
        }

        [Test]
        public async Task TestReplaceExactNonExistentRecordReturnsFalseDoesNotCreateRecord()
        {
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "1"), GetPoco(1, "2"));

            Assert.IsFalse(res);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithDifferentValueReturnsFalseDoesNotReplace()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "11"), GetPoco(1, "22"));

            Assert.IsFalse(res);
            Assert.AreEqual("1", (await PocoView.GetAsync(null, GetPoco(1)))!.Val);
        }

        [Test]
        public async Task TestReplaceExactExistingRecordWithSameValueReturnsTrueReplacesOld()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "1"));
            bool res = await PocoView.ReplaceAsync(null, GetPoco(1, "1"), GetPoco(1, "22"));

            Assert.IsTrue(res);
            Assert.AreEqual("22", (await PocoView.GetAsync(null, GetPoco(1)))!.Val);
        }

        [Test]
        public async Task TestUpsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            await PocoView.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res!.Val);
            }
        }

        [Test]
        public async Task TestUpsertAllOverwritesExistingData()
        {
            await PocoView.InsertAsync(null, GetPoco(2, "x"));
            await PocoView.InsertAsync(null, GetPoco(4, "y"));

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            await PocoView.UpsertAllAsync(null, records);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res!.Val);
            }
        }

        [Test]
        public async Task TestInsertAllCreatesRecords()
        {
            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<Poco> skipped = await PocoView.InsertAllAsync(null, records);

            CollectionAssert.IsEmpty(skipped);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));
                Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res!.Val);
            }
        }

        [Test]
        public async Task TestInsertAllDoesNotOverwriteExistingDataReturnsSkippedTuples()
        {
            var existing = new[] { GetPoco(2, "x"), GetPoco(4, "y") }.ToDictionary(x => x.Key);
            await PocoView.InsertAllAsync(null, existing.Values);

            var ids = Enumerable.Range(1, 10).ToList();
            var records = ids.Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            IList<Poco> skipped = await PocoView.InsertAllAsync(null, records);
            var skippedArr = skipped.OrderBy(x => x.Key).ToArray();

            Assert.AreEqual(2, skippedArr.Length);
            Assert.AreEqual(2, skippedArr[0].Key);
            Assert.AreEqual("2", skippedArr[0].Val);

            Assert.AreEqual(4, skippedArr[1].Key);
            Assert.AreEqual("4", skippedArr[1].Val);

            foreach (var id in ids)
            {
                var res = await PocoView.GetAsync(null, GetPoco(id));

                if (existing.TryGetValue(res!.Key, out var old))
                {
                    Assert.AreEqual(old.Val, res.Val);
                }
                else
                {
                    Assert.AreEqual(id.ToString(CultureInfo.InvariantCulture), res.Val);
                }
            }
        }

        [Test]
        public async Task TestInsertAllEmptyCollectionDoesNothingReturnsEmptyList()
        {
            var res = await PocoView.InsertAllAsync(null, Array.Empty<Poco>());
            CollectionAssert.IsEmpty(res);
        }

        [Test]
        public async Task TestUpsertAllEmptyCollectionDoesNothing()
        {
            await PocoView.UpsertAllAsync(null, Array.Empty<Poco>());
        }

        [Test]
        public async Task TestGetAllReturnsRecordsForExistingKeys()
        {
            var records = Enumerable
                .Range(1, 10)
                .Select(x => GetPoco(x, x.ToString(CultureInfo.InvariantCulture)));

            await PocoView.UpsertAllAsync(null, records);

            // TODO: Key order should be preserved by the server (IGNITE-16004).
            var res = await PocoView.GetAllAsync(null, Enumerable.Range(9, 4).Select(x => GetPoco(x)));
            var resArr = res.OrderBy(x => x?.Key).ToArray();

            Assert.AreEqual(2, res.Count);

            Assert.AreEqual(9, resArr[0]!.Key);
            Assert.AreEqual("9", resArr[0]!.Val);

            Assert.AreEqual(10, resArr[1]!.Key);
            Assert.AreEqual("10", resArr[1]!.Val);
        }

        [Test]
        public async Task TestGetAllNonExistentKeysReturnsEmptyList()
        {
            var res = await PocoView.GetAllAsync(null, new[] { GetPoco(-100) });

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public async Task TestGetAllEmptyKeysReturnsEmptyList()
        {
            var res = await PocoView.GetAllAsync(null, Array.Empty<Poco>());

            Assert.AreEqual(0, res.Count);
        }

        [Test]
        public async Task TestDeleteAllEmptyKeysReturnsEmptyList()
        {
            var skipped = await PocoView.DeleteAllAsync(null, Array.Empty<Poco>());

            Assert.AreEqual(0, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllNonExistentKeysReturnsAllKeys()
        {
            var skipped = await PocoView.DeleteAllAsync(null, new[] { GetPoco(1), GetPoco(2) });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExistingKeysReturnsEmptyListRemovesRecords()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });
            var skipped = await PocoView.DeleteAllAsync(null, new[] { GetPoco(1), GetPoco(2) });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(2)));
        }

        [Test]
        public async Task TestDeleteAllRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2"), GetPoco(3, "3") });
            var skipped = await PocoView.DeleteAllAsync(null, new[] { GetPoco(1), GetPoco(2), GetPoco(4) });

            Assert.AreEqual(1, skipped.Count);
            Assert.AreEqual(4, skipped[0].Key);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(2)));
            Assert.IsNotNull(await PocoView.GetAsync(null, GetPoco(3)));
        }

        [Test]
        public async Task TestDeleteAllExactNonExistentKeysReturnsAllKeys()
        {
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });

            Assert.AreEqual(2, skipped.Count);
            CollectionAssert.AreEquivalent(new long[] { 1, 2 }, skipped.Select(x => x.Key).ToArray());
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysDifferentValuesReturnsAllKeysDoesNotRemove()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "11"), GetPoco(2, "x") });

            Assert.AreEqual(2, skipped.Count);
        }

        [Test]
        public async Task TestDeleteAllExactExistingKeysReturnsEmptyListRemovesRecords()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2") });

            Assert.AreEqual(0, skipped.Count);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(2)));
        }

        [Test]
        public async Task TestDeleteAllExactRemovesExistingRecordsReturnsNonExistentKeys()
        {
            await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "2"), GetPoco(3, "3") });
            var skipped = await PocoView.DeleteAllExactAsync(null, new[] { GetPoco(1, "1"), GetPoco(2, "22") });

            Assert.AreEqual(1, skipped.Count);
            Assert.IsNull(await PocoView.GetAsync(null, GetPoco(1)));
            Assert.IsNotNull(await PocoView.GetAsync(null, GetPoco(2)));
            Assert.IsNotNull(await PocoView.GetAsync(null, GetPoco(3)));
        }

        [Test]
        public void TestUpsertAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await PocoView.UpsertAllAsync(null, new[] { GetPoco(1, "1"), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestGetAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await PocoView.GetAllAsync(null, new[] { GetPoco(1, "1"), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public void TestDeleteAllThrowsArgumentExceptionOnNullCollectionElement()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await PocoView.DeleteAllAsync(null, new[] { GetPoco(1, "1"), null! }));

            Assert.AreEqual("Record collection can't contain null elements.", ex!.Message);
        }

        [Test]
        public async Task TestLongValueRoundTrip([Values(
            long.MinValue,
            long.MinValue + 1,
            (long)int.MinValue - 1,
            int.MinValue,
            int.MinValue + 1,
            (long)short.MinValue - 1,
            short.MinValue,
            short.MinValue + 1,
            (long)byte.MinValue - 1,
            byte.MinValue,
            byte.MinValue + 1,
            -1,
            0,
            1,
            byte.MaxValue - 1,
            byte.MaxValue,
            (long)byte.MaxValue + 1,
            short.MaxValue - 1,
            short.MaxValue,
            (long)short.MaxValue + 1,
            int.MaxValue - 1,
            int.MaxValue,
            (long)int.MaxValue + 1,
            long.MaxValue - 1,
            long.MaxValue)] long key)
        {
            var val = key.ToString(CultureInfo.InvariantCulture);
            var poco = new Poco { Key = key, Val = val};
            await PocoView.UpsertAsync(null, poco);

            var keyTuple = new Poco { Key = key };
            var resTuple = (await PocoView.GetAsync(null, keyTuple))!;

            Assert.IsNotNull(resTuple);
            Assert.AreEqual(key, resTuple.Key);
            Assert.AreEqual(val, resTuple.Val);
        }
    }
}
