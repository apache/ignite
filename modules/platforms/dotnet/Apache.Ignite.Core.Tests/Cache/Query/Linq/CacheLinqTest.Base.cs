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
    using System.Collections;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /** Cache name. */
        private const string PersonOrgCacheName = "person_org";

        /** Cache schema. */
        private const string PersonOrgCacheSchema = "person_org_Schema";

        /** Cache name. */
        private const string PersonSecondCacheName = "person_cache";

        /** Cache schema. */
        private const string PersonSecondCacheSchema = "\"person_cache_SCHEMA\"";

        /** Role cache name: uses invalid characters to test name escaping. */
        private const string RoleCacheName = "role$ cache.";

        /** */
        private const int RoleCount = 3;

        /** */
        private const int PersonCount = 900;

        /** */
        private bool _runDbConsole;

        /** */
        private static readonly DateTime StartDateTime = new DateTime(2000, 5, 17, 15, 4, 5, DateTimeKind.Utc);

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _runDbConsole = false;  // set to true to open H2 console

            if (_runDbConsole)
                Environment.SetEnvironmentVariable("IGNITE_H2_DEBUG_CONSOLE", "true");

            Ignition.Start(GetConfig());
            Ignition.Start(GetConfig("grid2"));

            // Populate caches
            var cache = GetPersonCache();
            var personCache = GetSecondPersonCache();

            for (var i = 0; i < PersonCount; i++)
            {
                cache.Put(i, new Person(i, string.Format(" Person_{0}  ", i))
                {
                    Address = new Address {Zip = i, Street = "Street " + i, AliasTest = i},
                    OrganizationId = i%2 + 1000,
                    Birthday = StartDateTime.AddYears(i),
                    AliasTest = -i
                });

                var i2 = i + PersonCount;
                personCache.Put(i2, new Person(i2, "Person_" + i2)
                {
                    Address = new Address {Zip = i2, Street = "Street " + i2},
                    OrganizationId = i%2 + 1000,
                    Birthday = StartDateTime.AddYears(i)
                });
            }

            var orgCache = GetOrgCache();

            orgCache[1000] = new Organization {Id = 1000, Name = "Org_0"};
            orgCache[1001] = new Organization {Id = 1001, Name = "Org_1"};
            orgCache[1002] = new Organization {Id = 1002, Name = null};

            var roleCache = GetRoleCache();

            roleCache[new RoleKey(1, 101)] = new Role {Name = "Role_1", Date = StartDateTime};
            roleCache[new RoleKey(2, 102)] = new Role {Name = "Role_2", Date = StartDateTime.AddYears(1)};
            roleCache[new RoleKey(3, 103)] = new Role {Name = null, Date = StartDateTime.AddHours(5432)};
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private IgniteConfiguration GetConfig(string gridName = null)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person),
                    typeof(Organization), typeof(Address), typeof(Role), typeof(RoleKey), typeof(Numerics))
                {
                    NameMapper = GetNameMapper()
                },
                IgniteInstanceName = gridName
            };
        }

        /// <summary>
        /// Gets the name mapper.
        /// </summary>
        protected virtual IBinaryNameMapper GetNameMapper()
        {
            return new BinaryBasicNameMapper {IsSimpleName = false};
        }

        /// <summary>
        /// Gets the SqlEscapeAll setting.
        /// </summary>
        protected virtual bool GetSqlEscapeAll()
        {
            return false;
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            if (_runDbConsole)
                Thread.Sleep(Timeout.Infinite);
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets the person cache.
        /// </summary>
        /// <returns></returns>
        private ICache<int, Person> GetPersonCache()
        {
            return GetCacheOf<Person>();
        }

        /// <summary>
        /// Gets the org cache.
        /// </summary>
        /// <returns></returns>
        private ICache<int, Organization> GetOrgCache()
        {
            return GetCacheOf<Organization>();
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<int, T> GetCacheOf<T>()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<int, T>(new CacheConfiguration(PersonOrgCacheName,
                    new QueryEntity(typeof (int), typeof (Person))
                    {
                        Aliases = new[]
                        {
                            new QueryAlias("AliasTest", "Person_AliasTest"),
                            new QueryAlias("Address.AliasTest", "Addr_AliasTest")
                        },
                        KeyFieldName = "MyKey",
                        ValueFieldName = "MyValue",
                        Fields =
                        {
                            new QueryField("MyKey", typeof(int)),
                            new QueryField("MyValue", typeof(T)),
                        }
                    },
                    new QueryEntity(typeof (int), typeof (Organization)))
                {
                    CacheMode = CacheMode.Replicated,
                    SqlEscapeAll = GetSqlEscapeAll(),
                    SqlSchema = PersonOrgCacheSchema
                });
        }

        /// <summary>
        /// Gets the role cache.
        /// </summary>
        private ICache<RoleKey, Role> GetRoleCache()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<RoleKey, Role>(new CacheConfiguration(RoleCacheName,
                    new QueryEntity(typeof (RoleKey), typeof (Role)))
                {
                    CacheMode = CacheMode.Replicated,
                    SqlEscapeAll = GetSqlEscapeAll()
                });
        }

        /// <summary>
        /// Gets the second person cache.
        /// </summary>
        private ICache<int, Person> GetSecondPersonCache()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<int, Person>(
                    new CacheConfiguration(PersonSecondCacheName,
                        new QueryEntity(typeof(int), typeof(Person))
                        {
                            TableName = "CustomPersons"
                        })
                    {
                        CacheMode = CacheMode.Replicated,
                        SqlEscapeAll = GetSqlEscapeAll(),
                        SqlSchema = PersonSecondCacheSchema
                    });
        }

        /// <summary>
        /// Checks that function maps to SQL function properly.
        /// </summary>
        private static void CheckFunc<T, TR>(Expression<Func<T, TR>> exp, IQueryable<T> query,
            Func<TR, TR> localResultFunc = null)
        {
            localResultFunc = localResultFunc ?? (x => x);

            // Calculate result locally, using real method invocation
            var expected = query.ToArray().AsQueryable().Select(exp).Select(localResultFunc).OrderBy(x => x).ToArray();

            // Perform SQL query
            var actual = query.Select(exp).ToArray().OrderBy(x => x).ToArray();

            // Compare results
            CollectionAssert.AreEqual(expected, actual, new NumericComparer());

            // Perform intermediate anonymous type conversion to check type projection
            actual = query.Select(exp).Select(x => new {Foo = x}).ToArray().Select(x => x.Foo)
                .OrderBy(x => x).ToArray();

            // Compare results
            CollectionAssert.AreEqual(expected, actual, new NumericComparer());
        }

        /// <summary>
        /// Checks that function used in Where Clause maps to SQL function properly
        /// </summary>
        private static void CheckWhereFunc<TKey, TEntry>(IQueryable<ICacheEntry<TKey,TEntry>> query,
            Expression<Func<ICacheEntry<TKey, TEntry>,bool>> whereExpression)
        {
            // Calculate result locally, using real method invocation
            var expected = query
                .ToArray()
                .AsQueryable()
                .Where(whereExpression)
                .Select(entry => entry.Key)
                .OrderBy(x => x)
                .ToArray();

            // Perform SQL query
            var actual = query
                .Where(whereExpression)
                .Select(entry => entry.Key)
                .ToArray()
                .OrderBy(x => x)
                .ToArray();

            // Compare results
            CollectionAssert.AreEqual(expected, actual, new NumericComparer());
        }

        /// <summary>
        /// Tests conditinal statement
        /// </summary>
        private void TestConditional<T>(T even , T odd, Func<T,T,bool> comparer = null)
        {
            var persons = GetPersonCache().AsCacheQueryable();

            var res = persons
                .Select(x => new { x.Key, Foo = x.Key % 2 == 0 ? even : odd, x.Value })
                .OrderBy(x => x.Key)
                .ToArray();

            if (comparer != null)
            {
                Assert.IsTrue(comparer(even, res[0].Foo));
                Assert.IsTrue(comparer(odd, res[1].Foo));
            }
            else
            {
                Assert.AreEqual(even, res[0].Foo);
                Assert.AreEqual(odd, res[1].Foo);
            }
        }

        /// <summary>
        /// Tests conditinal statement for structs with default and null values
        /// </summary>
        private void TestConditionalWithNullableStructs<T>(T? defaultFalue = null) where T : struct
        {
            var def = defaultFalue ?? default(T);
            TestConditional(def, (T?) null);
        }

        public interface IPerson
        {
            int Age { get; set; }
            string Name { get; set; }
        }

        public class Person : IBinarizable, IPerson
        {
            public Person(int age, string name)
            {
                Age = age;
                Name = name;
            }

            [QuerySqlField(Name = "age1")] public int Age { get; set; }

            [QuerySqlField] public string Name { get; set; }

            [QuerySqlField] public Address Address { get; set; }

            [QuerySqlField] public int OrganizationId { get; set; }

            [QuerySqlField] public DateTime? Birthday { get; set; }

            [QuerySqlField] public int AliasTest { get; set; }

            [QuerySqlField] public bool Bool { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("age1", Age);
                writer.WriteString("name", Name);
                writer.WriteInt("OrganizationId", OrganizationId);
                writer.WriteObject("Address", Address);
                writer.WriteTimestamp("Birthday", Birthday);
                writer.WriteInt("AliasTest", AliasTest);
                writer.WriteBoolean("Bool", Bool);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Age = reader.ReadInt("age1");
                Name = reader.ReadString("name");
                OrganizationId = reader.ReadInt("OrganizationId");
                Address = reader.ReadObject<Address>("Address");
                Birthday = reader.ReadTimestamp("Birthday");
                AliasTest = reader.ReadInt("AliasTest");
                Bool = reader.ReadBoolean("Bool");
            }
        }

        public class Address
        {
            [QuerySqlField] public int Zip { get; set; }
            [QuerySqlField] public string Street { get; set; }
            [QuerySqlField] public int AliasTest { get; set; }
        }

        public class Organization
        {
            [QuerySqlField] public int Id { get; set; }
            [QuerySqlField] public string Name { get; set; }
        }

        public interface IRole
        {
            string Name { get; }
            DateTime Date { get; }
        }

        public class Role : IRole
        {
            [QuerySqlField] public string Name { get; set; }
            [QuerySqlField] public DateTime Date { get; set; }
        }

        public class Numerics
        {
            public Numerics(double val)
            {
                Double = val;
                Float = (float) val;
                Decimal = (decimal) val;
                Int = (int) val;
                Uint = (uint) val;
                Long = (long) val;
                Ulong = (ulong) val;
                Short = (short) val;
                Ushort = (ushort) val;
                Byte = (byte) val;
                Sbyte =  (sbyte) val;
            }

            [QuerySqlField] public double Double { get; set; }
            [QuerySqlField] public float Float { get; set; }
            [QuerySqlField] public decimal Decimal { get; set; }
            [QuerySqlField] public int Int { get; set; }
            [QuerySqlField] public uint Uint { get; set; }
            [QuerySqlField] public long Long { get; set; }
            [QuerySqlField] public ulong Ulong { get; set; }
            [QuerySqlField] public short Short { get; set; }
            [QuerySqlField] public ushort Ushort { get; set; }
            [QuerySqlField] public byte Byte { get; set; }
            [QuerySqlField] public sbyte Sbyte { get; set; }
        }

        public struct RoleKey : IEquatable<RoleKey>
        {
            private readonly int _foo;
            private readonly long _bar;

            public RoleKey(int foo, long bar)
            {
                _foo = foo;
                _bar = bar;
            }

            [QuerySqlField(Name = "_foo")]
            public int Foo
            {
                get { return _foo; }
            }

            [QuerySqlField(Name = "_bar")]
            public long Bar
            {
                get { return _bar; }
            }

            public bool Equals(RoleKey other)
            {
                return _foo == other._foo && _bar == other._bar;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is RoleKey && Equals((RoleKey) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (_foo*397) ^ _bar.GetHashCode();
                }
            }

            public static bool operator ==(RoleKey left, RoleKey right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(RoleKey left, RoleKey right)
            {
                return !left.Equals(right);
            }
        }

        /// <summary>
        /// Epsilon comparer.
        /// </summary>
        private class NumericComparer : IComparer
        {
            /** <inheritdoc /> */
            public int Compare(object x, object y)
            {
                if (Equals(x, y))
                    return 0;

                if (x is double)
                {
                    var dx = (double) x;
                    var dy = (double) y;

                    // Epsilon is proportional to the min value, but not too small.
                    const double epsilon = 2E-10d;
                    var min = Math.Min(Math.Abs(dx), Math.Abs(dy));
                    var relEpsilon = Math.Max(min*epsilon, epsilon);

                    // Compare with epsilon because some funcs return slightly different results.
                    return Math.Abs((double) x - (double) y) < relEpsilon ? 0 : 1;
                }

                return ((IComparable) x).CompareTo(y);
            }
        }
    }
}
