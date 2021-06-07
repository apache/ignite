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

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Platform.Model;
    using NUnit.Framework;

    /// <summary>
    /// IService .Net implementation.
    /// </summary>
    public class PlatformTestService : IJavaService, IService
    {
        /** Injected Ignite instance. */
        [InstanceResource] private readonly IIgnite _ignite;

        /** */
        private bool _initialized;

        /** */
        private bool _executed;

        /** */
        private bool _cancelled;

        /** */
        public PlatformTestService()
        {
            // No-Op.
        }


        /** */
        public PlatformTestService(IIgnite ignite)
        {
            _ignite = ignite;
        }

        /** <inheritDoc /> */
        public void Init(IServiceContext context)
        {
            _initialized = true;
        }

        /** <inheritDoc /> */
        public void Execute(IServiceContext context)
        {
            _executed = true;
        }

        /** <inheritDoc /> */
        public void Cancel(IServiceContext context)
        {
            _cancelled = true;
        }

        /** <inheritDoc /> */
        public bool isCancelled()
        {
            return _cancelled;
        }

        /** <inheritDoc /> */
        public bool isInitialized()
        {
            return _initialized;
        }

        /** <inheritDoc /> */
        public bool isExecuted()
        {
            return _executed;
        }

        /** <inheritDoc /> */
        public byte test(byte x)
        {
            return (byte) (x + 1);
        }

        /** <inheritDoc /> */
        public short test(short x)
        {
            return (short) (x + 1);
        }

        /** <inheritDoc /> */
        public int test(int x)
        {
            return x + 1;
        }

        /** <inheritDoc /> */
        public long test(long x)
        {
            return x + 1;
        }

        /** <inheritDoc /> */
        public float test(float x)
        {
            return x + 1.5f;
        }

        /** <inheritDoc /> */
        public double test(double x)
        {
            return x + 2.5;
        }

        /** <inheritDoc /> */
        public char test(char x)
        {
            return (char) (x + 1);
        }

        /** <inheritDoc /> */
        public string test(string x)
        {
            return x == null ? null : x + "!";
        }

        /** <inheritDoc /> */
        public bool test(bool x)
        {
            return !x;
        }

        /** <inheritDoc /> */
        public DateTime test(DateTime x)
        {
            Assert.AreEqual(new DateTime(1992, 1, 1, 0, 0, 0, 0), x);

            return x;
        }

        /** <inheritDoc /> */
        public Guid test(Guid x)
        {
            return x;
        }

        /** <inheritDoc /> */
        public byte? testWrapper(byte? x)
        {
            return (byte?) (x + 1);
        }

        /** <inheritDoc /> */
        public short? testWrapper(short? x)
        {
            return (short?) (x + 1);
        }

        /** <inheritDoc /> */
        public int? testWrapper(int? x)
        {
            return x + 1;
        }

        /** <inheritDoc /> */
        public long? testWrapper(long? x)
        {
            return x + 1;
        }

        /** <inheritDoc /> */
        public float? testWrapper(float? x)
        {
            return x + 1.5f;
        }

        /** <inheritDoc /> */
        public double? testWrapper(double? x)
        {
            return x + 2.5;
        }

        /** <inheritDoc /> */
        public char? testWrapper(char? x)
        {
            return (char?) (x + 1);
        }

        /** <inheritDoc /> */
        public bool? testWrapper(bool? x)
        {
            return !x;
        }

        /** <inheritDoc /> */
        public byte[] testArray(byte[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public short[] testArray(short[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public int[] testArray(int[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public long[] testArray(long[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public float[] testArray(float[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public double[] testArray(double[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public char[] testArray(char[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += (char) 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public string[] testArray(string[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] += 1;
            }

            return x;
        }

        /** <inheritDoc /> */
        public bool[] testArray(bool[] x)
        {
            for (int i = 0; i < x.Length; i++)
            {
                x[i] = !x[i];
            }

            return x;
        }

        /** <inheritDoc /> */
        public DateTime?[] testArray(DateTime?[] x)
        {
            if (x == null || x.Length != 1)
                throw new Exception("Expected array of length 1");

            // ReSharper disable once PossibleInvalidOperationException
            return new DateTime?[] {test((DateTime) x[0])};
        }

        /** <inheritDoc /> */
        public Guid?[] testArray(Guid?[] x)
        {
            return x;
        }

        /** <inheritDoc /> */
        public int test(int x, string y)
        {
            return x + 1;
        }

        /** <inheritDoc /> */
        public int test(string x, int y)
        {
            return y + 1;
        }

        /** <inheritDoc /> */
        public int? testNull(int? x)
        {
            return x == null ? null : x + 1;
        }

        /** <inheritDoc /> */
        public DateTime? testNullTimestamp(DateTime? x)
        {
            return x;
        }

        /** <inheritDoc /> */
        public Guid? testNullUUID(Guid? x)
        {
            return x;
        }

        /** <inheritDoc /> */
        public int testParams(params object[] args)
        {
            return args.Length;
        }

        /** <inheritDoc /> */
        public ServicesTest.PlatformComputeBinarizable testBinarizable(ServicesTest.PlatformComputeBinarizable x)
        {
            return x == null ? null : new ServicesTest.PlatformComputeBinarizable { Field = x.Field + 1};
        }

        /** <inheritDoc /> */
        public object[] testBinarizableArrayOfObjects(object[] x)
        {
            if (x == null)
                return null;

            for (int i = 0; i < x.Length; i++)
                x[i] = x[i] == null
                    ? null
                    : new ServicesTest.PlatformComputeBinarizable
                        {Field = ((ServicesTest.PlatformComputeBinarizable) x[i]).Field + 1};

            return x;
        }

        /** <inheritDoc /> */
        public IBinaryObject[] testBinaryObjectArray(IBinaryObject[] x)
        {
            for (int i = 0; i < x.Length; i++) {
                int field = x[i].GetField<int>("Field");

                x[i] = x[i].ToBuilder().SetField("Field", field + 1).Build();
            }

            return x;
        }

        /** <inheritDoc /> */
        public ServicesTest.PlatformComputeBinarizable[] testBinarizableArray(ServicesTest.PlatformComputeBinarizable[] x)
        {
            // ReSharper disable once CoVariantArrayConversion
            return (ServicesTest.PlatformComputeBinarizable[])testBinarizableArrayOfObjects(x);
        }

        /** <inheritDoc /> */
        public ICollection testBinarizableCollection(ICollection arg)
        {
            if (arg == null)
                return null;

            var res = new ArrayList(arg.Count);

            foreach (var x in arg)
            {
                res.Add(new ServicesTest.PlatformComputeBinarizable
                    {Field = ((ServicesTest.PlatformComputeBinarizable) x).Field + 1});
            }

            return res;
        }

        /** <inheritDoc /> */
        public IBinaryObject testBinaryObject(IBinaryObject x)
        {
            if (x == null)
                return null;

            return x.ToBuilder().SetField("field", 15).Build();
        }

        /** <inheritDoc /> */
        public Address testAddress(Address addr)
        {
            if (addr == null)
                return null;

            Assert.AreEqual("000", addr.Zip);
            Assert.AreEqual("Moscow", addr.Addr);

            addr.Zip = "127000";
            addr.Addr = "Moscow Akademika Koroleva 12";

            return addr;
        }

        /** <inheritDoc /> */
        public int testOverload(int count, Employee[] emps)
        {
            Assert.IsNotNull(emps);
            Assert.AreEqual(count, emps.Length);

            Assert.AreEqual("Sarah Connor", emps[0].Fio);
            Assert.AreEqual(1, emps[0].Salary);

            Assert.AreEqual("John Connor", emps[1].Fio);
            Assert.AreEqual(2, emps[1].Salary);

            return 42;
        }

        /** <inheritDoc /> */
        public int testOverload(int first, int second)
        {
            return first + second;
        }

        /** <inheritDoc /> */
        public int testOverload(int count, Parameter[] param)
        {
            Assert.IsNotNull(param);
            Assert.AreEqual(count, param.Length);

            Assert.AreEqual(1, param[0].Id);
            Assert.AreEqual(2, param[0].Values.Length);

            Assert.AreEqual(1, param[0].Values[0].Id);
            Assert.AreEqual(42, param[0].Values[0].Val);

            Assert.AreEqual(2, param[0].Values[1].Id);
            Assert.AreEqual(43, param[0].Values[1].Val);

            Assert.AreEqual(2, param[1].Id);
            Assert.AreEqual(2, param[1].Values.Length);

            Assert.AreEqual(3, param[1].Values[0].Id);
            Assert.AreEqual(44, param[1].Values[0].Val);

            Assert.AreEqual(4, param[1].Values[1].Id);
            Assert.AreEqual(45, param[1].Values[1].Val);

            return 43;
        }

        /** <inheritDoc /> */
        public Employee[] testEmployees(Employee[] emps)
        {
            if (emps == null)
                return null;

            Assert.AreEqual(2, emps.Length);

            Assert.AreEqual("Sarah Connor", emps[0].Fio);
            Assert.AreEqual(1, emps[0].Salary);

            Assert.AreEqual("John Connor", emps[1].Fio);
            Assert.AreEqual(2, emps[1].Salary);

            Employee kyle = new Employee();

            kyle.Fio = "Kyle Reese";
            kyle.Salary = 3;

            return new[] { kyle };

        }

        /** <inheritDoc /> */
        public Account[] testAccounts()
        {
            return new[] {
                new Account { Id = "123", Amount = 42},
                new Account { Id = "321", Amount = 0}
            };
        }

        /** <inheritDoc /> */
        public User[] testUsers()
        {
            return new[] {
                new User {Id = 1, Acl = ACL.Allow, Role = new Role {Name = "admin"}},
                new User {Id = 2, Acl = ACL.Deny, Role = new Role {Name = "user"}}
            };
        }

        /** <inheritDoc /> */
        public ICollection testDepartments(ICollection deps)
        {
            if (deps == null)
                return null;

            Assert.AreEqual(2, deps.Count);

            var arr = deps.OfType<Department>().ToArray();

            Assert.AreEqual("HR", arr[0].Name);
            Assert.AreEqual("IT", arr[1].Name);

            return new[] {new Department {Name = "Executive"}}.ToList();
        }

        /** <inheritDoc /> */
        public IDictionary testMap(IDictionary<Key, Value> dict)
        {
            if (dict == null)
                return null;

            Assert.IsTrue(dict.ContainsKey(new Key {Id = 1}));
            Assert.IsTrue(dict.ContainsKey(new Key {Id = 2}));

            Assert.AreEqual("value1", dict[new Key {Id = 1}].Val);
            Assert.AreEqual("value2", dict[new Key {Id = 2}].Val);

            return new Dictionary<Key, Value> {{new Key {Id = 3}, new Value {Val = "value3"}}};
        }

        /** <inheritDoc /> */
        public void testDateArray(DateTime?[] dates)
        {
            Assert.NotNull(dates);
            Assert.AreEqual(2, dates.Length);
            Assert.AreEqual(new DateTime(1982, 4, 1, 0, 0, 0), dates[0]);
            Assert.AreEqual(new DateTime(1991, 10, 1, 0, 0, 0), dates[1]);
        }

        /** <inheritDoc /> */
        public DateTime testDate(DateTime date)
        {
            Assert.AreEqual(new DateTime(1982, 4, 1, 0, 0, 0), date);

            // TODO: Check non UTC Dates
            return new DateTime(1991, 10, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        /** <inheritDoc /> */
        public void testUTCDateFromCache()
        {
            var cache = _ignite.GetCache<int, DateTime>("net-dates");

            cache.Put(3, new DateTime(1982, 4, 1, 0, 0, 0, DateTimeKind.Utc));
            cache.Put(4, new DateTime(1991, 10, 1, 0, 0, 0, DateTimeKind.Utc));

            Assert.AreEqual(new DateTime(1982, 4, 1, 0, 0, 0), cache.Get(1));
            Assert.AreEqual(new DateTime(1991, 10, 1, 0, 0, 0), cache.Get(2));
        }

        /** <inheritDoc /> */
        public void testLocalDateFromCache()
        {
            var cache = _ignite.GetCache<int, DateTime>("net-dates");

            var ts1 = new DateTime(1982, 4, 1, 1, 0, 0, 0, DateTimeKind.Local).ToUniversalTime();
            var ts2 = new DateTime(1982, 3, 31, 22, 0, 0, 0, DateTimeKind.Local).ToUniversalTime();

            Assert.AreEqual(ts1, cache.Get(5));
            Assert.AreEqual(ts2, cache.Get(6));

            cache.Put(7, ts1);
            cache.Put(8, ts2);
        }

        /** <inheritDoc /> */
        public void testException(string exCls)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void sleep(long delayMs)
        {
            throw new NotImplementedException();
        }
    }
}
