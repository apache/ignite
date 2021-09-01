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

// ReSharper disable SuspiciousTypeConversion.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable StringIndexOfIsCultureSpecific.1
// ReSharper disable StringIndexOfIsCultureSpecific.2
// ReSharper disable StringCompareToIsCultureSpecific
// ReSharper disable StringCompareIsCultureSpecific.1
// ReSharper disable UnusedMemberInSuper.Global
namespace Apache.Ignite.Core.Tests.Cache.Query.Linq
{
    using System;
    using System.Linq;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests aggregates.
        /// </summary>
        [Test]
        public void TestAggregates()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            Assert.AreEqual(PersonCount, cache.Count());
            Assert.AreEqual(PersonCount, cache.Select(x => x.Key).Count());
            Assert.AreEqual(2, cache.Select(x => x.Value.OrganizationId).Distinct().Count());

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            Assert.Throws<NotSupportedException>(() => cache.Select(x => new { x.Key, x.Value }).Count());

            // Min/max/sum/avg
            var ints = cache.Select(x => x.Key);
            Assert.AreEqual(0, ints.Min());
            Assert.AreEqual(PersonCount - 1, ints.Max());
            Assert.AreEqual(ints.ToArray().Sum(), ints.Sum());
            Assert.AreEqual((int)ints.ToArray().Average(), (int)ints.Average());

            var dupInts = ints.Select(x => x / 10);  // duplicate values
            CollectionAssert.AreEquivalent(dupInts.ToArray().Distinct().ToArray(), dupInts.Distinct().ToArray());
            Assert.AreEqual(dupInts.ToArray().Distinct().Sum(), dupInts.Distinct().Sum());

            // All/any
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            Assert.IsFalse(ints.Where(x => x > -5).Any(x => x > PersonCount && x > 0));
            Assert.IsTrue(ints.Any(x => x < PersonCount / 2));

            // Skip/take
            var keys = cache.Select(x => x.Key).OrderBy(x => x);
            Assert.AreEqual(new[] { 0, 1 }, keys.Take(2).ToArray());
            Assert.AreEqual(new[] { 1, 2 }, keys.Skip(1).Take(2).ToArray());
            Assert.AreEqual(new[] { PersonCount - 2, PersonCount - 1 }, keys.Skip(PersonCount - 2).ToArray());
        }

        /// <summary>
        /// Tests aggregates with all clause.
        /// </summary>
        [Test]
        public void TestAggregatesAll()
        {
            var ints = GetPersonCache().AsCacheQueryable().Select(x => x.Key);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<NotSupportedException>(() => ints.Where(x => x > -10)
                .All(x => x < PersonCount && x >= 0));

            Assert.IsTrue(ex.Message.StartsWith("Operator is not supported: All"));
        }
    }
}