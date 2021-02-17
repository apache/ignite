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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Platform.Model;

    /// <summary>
    /// Explicit service proxy over dynamic variable.
    /// </summary>
    public class JavaServiceDynamicProxy : IJavaService
    {
        /** */
        private readonly dynamic _svc;

        /** */
        public JavaServiceDynamicProxy(dynamic svc)
        {
            _svc = svc;
        }

        /** <inheritDoc /> */
        public bool isCancelled()
        {
            return _svc.isCancelled();
        }

        /** <inheritDoc /> */
        public bool isInitialized()
        {
            return _svc.isInitialized();
        }

        /** <inheritDoc /> */
        public bool isExecuted()
        {
            return _svc.isExecuted();
        }

        /** <inheritDoc /> */
        public byte test(byte x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public short test(short x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public int test(int x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public long test(long x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public float test(float x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public double test(double x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public char test(char x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public string test(string x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public bool test(bool x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public DateTime test(DateTime x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public Guid test(Guid x)
        {
            return _svc.test(x);
        }

        /** <inheritDoc /> */
        public byte? testWrapper(byte? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public short? testWrapper(short? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public int? testWrapper(int? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public long? testWrapper(long? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public float? testWrapper(float? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public double? testWrapper(double? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public char? testWrapper(char? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public bool? testWrapper(bool? x)
        {
            return _svc.testWrapper(x);
        }

        /** <inheritDoc /> */
        public byte[] testArray(byte[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public short[] testArray(short[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public int[] testArray(int[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public long[] testArray(long[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public float[] testArray(float[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public double[] testArray(double[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public char[] testArray(char[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public string[] testArray(string[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public bool[] testArray(bool[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public DateTime?[] testArray(DateTime?[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public Guid?[] testArray(Guid?[] x)
        {
            return _svc.testArray(x);
        }

        /** <inheritDoc /> */
        public int test(int x, string y)
        {
            return _svc.test(x, y);
        }

        /** <inheritDoc /> */
        public int test(string x, int y)
        {
            return _svc.test(x, y);
        }

        /** <inheritDoc /> */
        public int? testNull(int? x)
        {
            return _svc.testNull(x);
        }

        /** <inheritDoc /> */
        public DateTime? testNullTimestamp(DateTime? x)
        {
            return _svc.testNullTimestamp(x);
        }

        /** <inheritDoc /> */
        public Guid? testNullUUID(Guid? x)
        {
            return _svc.testNullUUID(x);
        }

        /** <inheritDoc /> */
        public int testParams(params object[] args)
        {
            return _svc.testParams(args);
        }

        /** <inheritDoc /> */
        public PlatformComputeBinarizable testBinarizable(PlatformComputeBinarizable x)
        {
            return _svc.testBinarizable(x);
        }

        /** <inheritDoc /> */
        public object[] testBinarizableArrayOfObjects(object[] x)
        {
            return _svc.testBinarizableArrayOfObjects(x);
        }

        /** <inheritDoc /> */
        public IBinaryObject[] testBinaryObjectArray(IBinaryObject[] x)
        {
            return _svc.testBinaryObjectArray(x);
        }

        /** <inheritDoc /> */
        public PlatformComputeBinarizable[] testBinarizableArray(PlatformComputeBinarizable[] x)
        {
            return _svc.testBinarizableArray(x);
        }

        /** <inheritDoc /> */
        public ICollection testBinarizableCollection(ICollection x)
        {
            return _svc.testBinarizableCollection(x);
        }

        /** <inheritDoc /> */
        public IBinaryObject testBinaryObject(IBinaryObject x)
        {
            return _svc.testBinaryObject(x);
        }

        /** <inheritDoc /> */
        public Address testAddress(Address addr)
        {
            return _svc.testAddress(addr);
        }

        /** <inheritDoc /> */
        public int testOverload(int count, Employee[] emps)
        {
            return _svc.testOverload(count, emps);
        }

        /** <inheritDoc /> */
        public int testOverload(int first, int second)
        {
            return _svc.testOverload(first, second);
        }

        /** <inheritDoc /> */
        public int testOverload(int count, Parameter[] param)
        {
            return _svc.testOverload(count, param);
        }

        /** <inheritDoc /> */
        public Employee[] testEmployees(Employee[] emps)
        {
            return _svc.testEmployees(emps);
        }

        public Account[] testAccounts()
        {
            return _svc.testAccounts();
        }

        public User[] testUsers()
        {
            return _svc.testUsers();
        }

        /** <inheritDoc /> */
        public ICollection testDepartments(ICollection deps)
        {
            return _svc.testDepartments(deps);
        }

        /** <inheritDoc /> */
        public IDictionary testMap(IDictionary<Key, Value> dict)
        {
            return _svc.testMap(dict);
        }

        /** <inheritDoc /> */
        public void testDateArray(DateTime?[] dates)
        {
            _svc.testDateArray(dates);
        }

        /** <inheritDoc /> */
        public DateTime testDate(DateTime date)
        {
            return _svc.testDate(date);
        }

        /** <inheritDoc /> */
        public void testUTCDateFromCache()
        {
            _svc.testUTCDateFromCache();
        }

        /** <inheritDoc /> */
        public void testLocalDateFromCache()
        {
            _svc.testLocalDateFromCache();
        }

        /** <inheritDoc /> */
        public void testException(string exceptionClass)
        {
            _svc.testException(exceptionClass);
        }

        /** <inheritDoc /> */
        public void sleep(long delayMs)
        {
            _svc.sleep(delayMs);
        }

        /** <inheritDoc /> */
        public void startReceiveMessage()
        {
            _svc.startReceiveMessage();
        }

        /** <inheritDoc /> */
        public bool testMessagesReceived()
        {
            return _svc.testMessagesReceived();
        }

        /** <inheritDoc /> */
        public void testSendMessage()
        {
            _svc.testSendMessage();
        }

        /** <inheritDoc /> */
        public void putValsForCache()
        {
            _svc.putValsForCache();
        }

        /** <inheritDoc /> */
        public object testRoundtrip(object x)
        {
            return x;
        }

        /** <inheritDoc /> */
        public object contextAttribute(string name)
        {
            return _svc.contextAttribute(name);
        }
    }
}
