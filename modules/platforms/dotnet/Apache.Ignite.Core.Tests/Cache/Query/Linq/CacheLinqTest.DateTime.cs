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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests date time.
        /// </summary>
        [Test]
        public void TestDateTime()
        {
            var roles = GetRoleCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            // Invalid dateTime
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<BinaryObjectException>(() =>
                roles.Where(x => x.Value.Date > DateTime.Now).ToArray());
            Assert.AreEqual("DateTime is not UTC. Only UTC DateTime can be used for interop with other platforms.",
                ex.Message);

            // Test retrieval
            var dates = roles.OrderBy(x => x.Value.Date).Select(x => x.Value.Date);
            var expDates = GetRoleCache().Select(x => x.Value.Date).OrderBy(x => x).ToArray();
            Assert.AreEqual(expDates, dates.ToArray());

            // Filtering
            Assert.AreEqual(1, persons.Count(x => x.Value.Birthday == StartDateTime));
            Assert.AreEqual(PersonCount, persons.Count(x => x.Value.Birthday >= StartDateTime));
            Assert.Greater(persons.Count(x => x.Value.Birthday > DateTime.UtcNow), 1);

            // Joins
            var join =
                from role in roles
                join person in persons on role.Value.Date equals person.Value.Birthday
                select person;

            Assert.AreEqual(2, join.Count());

            // Functions
            var strings = dates.Select(x => x.ToString("dd MM YYYY HH:mm:ss")).ToArray();
            Assert.AreEqual(new[] { "17 05 2000 15:04:05", "29 12 2000 23:04:05", "17 05 2001 15:04:05" }, strings);

            // Properties
            Assert.AreEqual(new[] { 2000, 2000, 2001 }, dates.Select(x => x.Year).ToArray());
            Assert.AreEqual(new[] { 5, 12, 5 }, dates.Select(x => x.Month).ToArray());
            Assert.AreEqual(new[] { 17, 29, 17 }, dates.Select(x => x.Day).ToArray());
            Assert.AreEqual(expDates.Select(x => x.DayOfYear).ToArray(), dates.Select(x => x.DayOfYear).ToArray());
            Assert.AreEqual(expDates.Select(x => x.DayOfWeek).ToArray(), dates.Select(x => x.DayOfWeek).ToArray());
            Assert.AreEqual(new[] { 15, 23, 15 }, dates.Select(x => x.Hour).ToArray());
            Assert.AreEqual(new[] { 4, 4, 4 }, dates.Select(x => x.Minute).ToArray());
            Assert.AreEqual(new[] { 5, 5, 5 }, dates.Select(x => x.Second).ToArray());
        }
    }
}