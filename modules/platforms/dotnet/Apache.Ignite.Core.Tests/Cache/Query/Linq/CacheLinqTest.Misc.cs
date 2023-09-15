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
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Linq;
    using NUnit.Framework;
    using NUnit.Framework.Constraints;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests the empty query.
        /// </summary>
        [Test]
        public void TestEmptyQuery()
        {
            // There are both persons and organizations in the same cache, but query should only return specific type
            Assert.AreEqual(PersonCount, GetPersonCache().AsCacheQueryable().ToArray().Length);
            Assert.AreEqual(RoleCount, GetRoleCache().AsCacheQueryable().ToArray().Length);
        }

        /// <summary>
        /// Tests the single field query.
        /// </summary>
        [Test]
        public void TestSingleFieldQuery()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // Multiple values
            Assert.AreEqual(new[] { 0, 1, 2 },
                cache.Where(x => x.Key < 3).OrderBy(x => x.Key).Select(x => x.Value.Address.Zip).ToArray());

            // Single value
            Assert.AreEqual(0, cache.Where(x => x.Key < 0).Select(x => x.Value.Age).FirstOrDefault());
            Assert.AreEqual(3, cache.Where(x => x.Key == 3).Select(x => x.Value.Age).FirstOrDefault());
            Assert.AreEqual(3, cache.Where(x => x.Key == 3).Select(x => x.Value).Single().Age);
            Assert.AreEqual(3, cache.Select(x => x.Key).Single(x => x == 3));
            Assert.AreEqual(7,
                cache.Select(x => x.Value)
                    .Where(x => x.Age == 7)
                    .Select(x => x.Address)
                    .Where(x => x.Zip > 0)
                    .Select(x => x.Zip)
                    .Single());
        }

        /// <summary>
        /// Tests the field projection.
        /// </summary>
        [Test]
        public void TestFieldProjection()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // Project whole cache entry to anonymous class
            Assert.AreEqual(5, cache.Where(x => x.Key == 5).Select(x => new { Foo = x }).Single().Foo.Key);
        }

        /// <summary>
        /// Tests the multi field query.
        /// </summary>
        [Test]
        public void TestMultiFieldQuery()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // Test anonymous type (ctor invoke)
            var data = cache
                .Select(x => new { Id = x.Key + 20, Age_ = x.Value.Age + 10, Addr = x.Value.Address })
                .Where(x => x.Id < 25)
                .ToArray();

            Assert.AreEqual(5, data.Length);

            foreach (var t in data)
            {
                Assert.AreEqual(t.Age_ - 10, t.Id - 20);
                Assert.AreEqual(t.Age_ - 10, t.Addr.Zip);
            }
        }

        /// <summary>
        /// Tests the scalar query.
        /// </summary>
        [Test]
        public void TestScalarQuery()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            Assert.AreEqual(PersonCount - 1, cache.Max(x => x.Value.Age));
            Assert.AreEqual(0, cache.Min(x => x.Value.Age));

            Assert.AreEqual(21, cache.Where(x => x.Key > 5 && x.Value.Age < 9).Select(x => x.Value.Age).Sum());

            Assert.AreEqual(PersonCount, cache.Count());
            Assert.AreEqual(PersonCount, cache.Count(x => x.Key < PersonCount));
            
            Assert.AreEqual(PersonCount, cache.LongCount());
            Assert.AreEqual(PersonCount, cache.LongCount(x => x.Key < PersonCount));
        }

        /// <summary>
        /// Tests conditions.
        /// </summary>
        [Test]
        public void TestConditions()
        {
            TestConditional("even", "odd");
            TestConditional(new Address { Zip = 99999 }, new Address { Zip = 7777777 }, (a1, a2) => a1.Zip == a2.Zip);
            TestConditional(new RoleKey(int.MaxValue, long.MinValue), new RoleKey(int.MinValue, long.MaxValue));
            TestConditionalWithNullableStructs<int>();
            TestConditionalWithNullableStructs<uint>();
            TestConditionalWithNullableStructs<Guid>();
            TestConditionalWithNullableStructs<byte>();
            TestConditionalWithNullableStructs<sbyte>();
            TestConditionalWithNullableStructs<short>();
            TestConditionalWithNullableStructs<ushort>();
            TestConditionalWithNullableStructs<bool>();
            TestConditionalWithNullableStructs<long>();
            TestConditionalWithNullableStructs<ulong>();
            TestConditionalWithNullableStructs<double>();
            TestConditionalWithNullableStructs<float>();
            TestConditionalWithNullableStructs<decimal>();
            TestConditionalWithNullableStructs<DateTime>(DateTime.Parse("1983-03-14 13:20:15.999999").ToUniversalTime());

            var charException = Assert.Throws<NotSupportedException>(() => TestConditionalWithNullableStructs<char>());
            Assert.AreEqual("Type is not supported for SQL mapping: System.Char", charException.Message);

            var roles = GetRoleCache().AsCacheQueryable();
            CheckFunc(x => x.Value.Name ?? "def_name", roles);
        }

        /// <summary>
        /// Tests the SelectMany from field collection.
        /// </summary>
        [Test]
        public void TestSelectManySameTable()
        {
            var persons = GetPersonCache().AsCacheQueryable();

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<NotSupportedException>(() => persons.SelectMany(x => x.Value.Name).ToArray());

            Assert.IsTrue(ex.Message.StartsWith("FROM clause must be IQueryable: from Char"));
        }

        /// <summary>
        /// Tests nulls.
        /// </summary>
        [Test]
        public void TestNulls()
        {
            var roles = GetRoleCache().AsCacheQueryable();

            var nullNameRole = roles.Single(x => x.Value.Name == null);
            Assert.AreEqual(null, nullNameRole.Value.Name);

            var nonNullNameRoles = roles.Where(x => x.Value.Name != null);
            Assert.AreEqual(RoleCount - 1, nonNullNameRoles.Count());
        }

        /// <summary>
        /// Tests aliases.
        /// </summary>
        [Test]
        public void TestAliases()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            var res = cache.Where(x => x.Key == 1)
                .Select(x => new { X = x.Value.AliasTest, Y = x.Value.Address.AddressAliasTest })
                .Single();

            Assert.AreEqual(new { X = -1, Y = 1 }, res);
        }

        /// <summary>
        /// Tests the cache of primitive types.
        /// </summary>
        [Test]
        public void TestPrimitiveCache()
        {
            // Create partitioned cache
            var cache = Ignition.GetIgnite()
                .GetOrCreateCache<int, string>(
                    new CacheConfiguration("primitiveCache",
                        new QueryEntity(typeof(int), typeof(string)))
                    {
                        CacheMode = CacheMode.Replicated,
                        SqlEscapeAll = GetSqlEscapeAll()
                    });

            var qry = cache.AsCacheQueryable();

            // Populate
            const int count = 100;
            cache.PutAll(Enumerable.Range(0, count).ToDictionary(x => x, x => x.ToString()));

            // Test
            Assert.AreEqual(count, qry.ToArray().Length);
            Assert.AreEqual(10, qry.Where(x => x.Key < 10).ToArray().Length);
            Assert.AreEqual(1, qry.Count(x => x.Value.Contains("99")));
        }

        /// <summary>
        /// Tests the local query.
        /// </summary>
        [Test]
        public void TestLocalQuery()
        {
            // Create partitioned cache
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, int>(new CacheConfiguration("partCache",
                    new QueryEntity(typeof(int), typeof(int)))
                {
                    SqlEscapeAll = GetSqlEscapeAll()
                });

            // Populate
            const int count = 100;
            cache.PutAll(Enumerable.Range(0, count).ToDictionary(x => x, x => x));

            // Non-local query returns all records
            Assert.AreEqual(count, cache.AsCacheQueryable(false).ToArray().Length);

            // Local query returns only some of the records
            var localCount = cache.AsCacheQueryable(true).ToArray().Length;
            Assert.Less(localCount, count);
            Assert.Greater(localCount, 0);
        }

        /// <summary>
        /// Tests the table name inference.
        /// </summary>
        [Test]
        public void TestTableNameInference()
        {
            // Try with multi-type cache: explicit type is required
            var cache = GetCacheOf<IPerson>();

            Assert.Throws<CacheException>(() => cache.AsCacheQueryable());

            var names = cache.AsCacheQueryable(false, "Person").Select(x => x.Value.Name).ToArray();

            Assert.AreEqual(PersonCount, names.Length);

            // With single-type cache, interface inference works
            var roleCache = Ignition.GetIgnite().GetCache<object, IRole>(RoleCacheName).AsCacheQueryable();

            var roleNames = roleCache.Select(x => x.Value.Name).OrderBy(x => x).ToArray();

            CollectionAssert.AreEquivalent(new[] { "Role_1", "Role_2", null }, roleNames);

            // Check non-queryable cache
            var nonQueryableCache = Ignition.GetIgnite().GetOrCreateCache<Role, Person>("nonQueryable");

            Assert.Throws<CacheException>(() => nonQueryableCache.AsCacheQueryable());
        }

        /// <summary>
        /// Tests the distributed joins.
        /// </summary>
        [Test]
        public void TestDistributedJoins()
        {
            var ignite = Ignition.GetIgnite();

            // Create and populate partitioned caches
            var personCache = ignite.CreateCache<int, Person>(new CacheConfiguration("partitioned_persons",
                new QueryEntity(typeof(int), typeof(Person)))
            {
                SqlEscapeAll = GetSqlEscapeAll()
            });

            personCache.PutAll(GetSecondPersonCache().ToDictionary(x => x.Key, x => x.Value));

            var roleCache = ignite.CreateCache<int, Role>(new CacheConfiguration("partitioned_roles",
                new QueryEntity(typeof(int), typeof(Role)))
            {
                SqlEscapeAll = GetSqlEscapeAll()
            });

            roleCache.PutAll(GetRoleCache().ToDictionary(x => x.Key.Foo, x => x.Value));

            // Test non-distributed join: returns partial results
            var persons = personCache.AsCacheQueryable();
            var roles = roleCache.AsCacheQueryable();

            // we have role.Keys = [1, 2, 3] and persons.Key = [0, .. PersonCount)
            var res = persons.Join(roles, person => person.Key % 2, role => role.Key, (person, role) => role)
                .ToArray();

            Assert.IsTrue(PersonCount / 2 > res.Length);

            // Test distributed join: returns complete results
            persons = personCache.AsCacheQueryable(new QueryOptions { EnableDistributedJoins = true });
            roles = roleCache.AsCacheQueryable(new QueryOptions { EnableDistributedJoins = true });

            res = persons.Join(roles, person => person.Key % 2, role => role.Key, (person, role) => role)
                .ToArray();

            Assert.AreEqual(PersonCount / 2, res.Length);
        }

        /// <summary>
        /// Tests the query timeout.
        /// </summary>
        [Test]
        public void TestTimeout()
        {
            var persons = GetPersonCache().AsCacheQueryable(new QueryOptions
            {
                Timeout = TimeSpan.FromMilliseconds(1),
                EnableDistributedJoins = true
            });

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<CacheException>(() =>
            {
                for (var i = 0; i < 100; i++)
                {
                    persons.SelectMany(p => GetRoleCache().AsCacheQueryable())
                        .Where(p => p.Value.Name.Contains("e")).ToArray();
                }
            });

            Assert.IsTrue(ex.ToString().Contains("QueryCancelledException: The query was cancelled while executing."));
        }

        /// <summary>
        /// Tests that <see cref="InvocationExpression"/> is not supported.
        /// </summary>
        [Test]
        public void TestInvokeThrowsNotSupportedException()
        {
            var constraint = new ReusableConstraint(Is.TypeOf<NotSupportedException>()
                .And.Message.StartsWith("The LINQ expression '")
                .And.Message.Contains("Invoke")
                .And.Message.Contains(
                    "could not be translated. Either rewrite the query in a form that can be translated, or switch to client evaluation explicitly by inserting a call to either AsEnumerable() or ToList()."));

            Func<ICacheEntry<int, Person>, bool> filter = entry => false;
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            Assert.Throws(constraint, () => GetPersonCache().AsCacheQueryable()
                .Where(x => filter(x))
                .ToList());

            Func<ICacheEntry<int, Person>, int> selector = x => x.Key;
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            Assert.Throws(constraint, () => GetPersonCache().AsCacheQueryable()
                .Select(x => selector(x))
                .FirstOrDefault());
        }

        /// <summary>
        /// Tests queries when cache key/val types are generic.
        /// </summary>
        [Test]
        public void TestGenericCacheTypes()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                QueryEntities = new[] {new QueryEntity(typeof(GenericTest<int>), typeof(GenericTest2<string>))}
            };

            var cache = Ignition.GetIgnite().GetOrCreateCache<GenericTest<int>, GenericTest2<string>>(cfg);
            var key = new GenericTest<int>(1);
            var value = new GenericTest2<string>("foo");
            cache[key] = value;

            var query = cache.AsCacheQueryable()
                .Where(x => x.Key.Foo == key.Foo && x.Value.Bar == value.Bar)
                .Select(x => x.Value.Bar);

            var sql = query.ToCacheQueryable().GetFieldsQuery().Sql;
            var res = query.ToList();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(value.Bar, res[0]);

            var expectedSql = string.Format("select _T0.BAR from \"{0}\".GENERICTEST2 as", cache.Name);
            StringAssert.StartsWith(expectedSql, sql);
        }

        /// <summary>
        /// Tests queries when cache key/val types are generic.
        /// </summary>
        [Test]
        public void TestNestedGenericCacheTypes()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                QueryEntities = new[] {new QueryEntity(typeof(int), typeof(GenericTest<GenericTest2<string>>))}
            };

            var cache = Ignition.GetIgnite().GetOrCreateCache<int, GenericTest<GenericTest2<string>>>(cfg);
            var key = 1;
            var value = new GenericTest<GenericTest2<string>>(new GenericTest2<string>("foo"));
            cache[key] = value;

            var query = cache.AsCacheQueryable()
                .Where(x => x.Value.Foo.Bar == value.Foo.Bar)
                .Select(x => x.Value.Foo);

            var sql = query.ToCacheQueryable().GetFieldsQuery().Sql;
            var res = query.ToList();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(value.Foo.Bar, res[0].Bar);

            var expectedSql = string.Format("select _T0.FOO from \"{0}\".GENERICTEST as", cache.Name);
            StringAssert.StartsWith(expectedSql, sql);
        }

        /// <summary>
        /// Tests queries when cache val type has two generic type arguments.
        /// </summary>
        [Test]
        public void TestTwoGenericArgumentsCacheType()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                QueryEntities = new[] {new QueryEntity(typeof(int), typeof(GenericTest3<int, string, bool>))}
            };

            var cache = Ignition.GetIgnite().GetOrCreateCache<int, GenericTest3<int, string, bool>>(cfg);
            var key = 1;
            var value = new GenericTest3<int, string, bool>(2, "3", true);
            cache[key] = value;

            var query = cache.AsCacheQueryable()
                .Where(x => x.Value.Baz == value.Baz && x.Value.Qux == value.Qux && x.Value.Quz)
                .Select(x => x.Value.Baz);

            var sql = query.ToCacheQueryable().GetFieldsQuery().Sql;
            var res = query.ToList();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(value.Baz, res[0]);

            var expectedSql = string.Format("select _T0.BAZ from \"{0}\".GENERICTEST3 as", cache.Name);
            StringAssert.StartsWith(expectedSql, sql);
        }

        /// <summary>
        /// Generic query type.
        /// </summary>
        private class GenericTest<T>
        {
            /** */
            public GenericTest(T foo)
            {
                Foo = foo;
            }

            /** */
            [QuerySqlField]
            public T Foo { get; set; }
        }

        /// <summary>
        /// Generic query type.
        /// </summary>
        private class GenericTest2<T>
        {
            /** */
            public GenericTest2(T bar)
            {
                Bar = bar;
            }

            /** */
            [QuerySqlField]
            public T Bar { get; set; }
        }

        /// <summary>
        /// Generic query type with two generic arguments.
        /// </summary>
        private class GenericTest3<T, T2, T3>
        {
            /** */
            public GenericTest3(T baz, T2 qux, T3 quz)
            {
                Baz = baz;
                Qux = qux;
                Quz = quz;
            }

            /** */
            [QuerySqlField]
            public T Baz { get; set; }
            
            /** */
            [QuerySqlField]
            public T2 Qux { get; set; }
            
            /** */
            [QuerySqlField]
            public T3 Quz { get; set; }
        }
    }
}
