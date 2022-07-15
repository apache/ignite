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
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Platform.Model;

    /// <summary>
    /// Java service proxy interface.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public interface IJavaService
    {
        /** */
        bool isCancelled();

        /** */
        bool isInitialized();

        /** */
        bool isExecuted();

        /** */
        byte test(byte x);

        /** */
        short test(short x);

        /** */
        int test(int x);

        /** */
        long test(long x);

        /** */
        float test(float x);

        /** */
        double test(double x);

        /** */
        char test(char x);

        /** */
        string test(string x);

        /** */
        bool test(bool x);

        /** */
        DateTime test(DateTime x);

        /** */
        Guid test(Guid x);

        /** */
        byte? testWrapper(byte? x);

        /** */
        short? testWrapper(short? x);

        /** */
        int? testWrapper(int? x);

        /** */
        long? testWrapper(long? x);

        /** */
        float? testWrapper(float? x);

        /** */
        double? testWrapper(double? x);

        /** */
        char? testWrapper(char? x);

        /** */
        bool? testWrapper(bool? x);

        /** */
        byte[] testArray(byte[] x);

        /** */
        short[] testArray(short[] x);

        /** */
        int[] testArray(int[] x);

        /** */
        long[] testArray(long[] x);

        /** */
        float[] testArray(float[] x);

        /** */
        double[] testArray(double[] x);

        /** */
        char[] testArray(char[] x);

        /** */
        string[] testArray(string[] x);

        /** */
        bool[] testArray(bool[] x);

        /** */
        DateTime?[] testArray(DateTime?[] x);

        /** */
        Guid?[] testArray(Guid?[] x);

        /** */
        int test(int x, string y);

        /** */
        int test(string x, int y);

        /** */
        int? testNull(int? x);

        /** */
        DateTime? testNullTimestamp(DateTime? x);

        /** */
        Guid? testNullUUID(Guid? x);

        /** */
        int testParams(params object[] args);

        /** */
        PlatformComputeBinarizable testBinarizable(PlatformComputeBinarizable x);

        /** */
        object[] testBinarizableArrayOfObjects(object[] x);

        /** */
        IBinaryObject[] testBinaryObjectArray(IBinaryObject[] x);

        /** */
        PlatformComputeBinarizable[] testBinarizableArray(PlatformComputeBinarizable[] x);

        /** */
        ICollection testBinarizableCollection(ICollection x);

        /** */
        IBinaryObject testBinaryObject(IBinaryObject x);

        /** */
        Address testAddress(Address addr);

        /** */
        int testOverload(int count, Employee[] emps);

        /** */
        int testOverload(int first, int second);

        /** */
        int testOverload(int count, Parameter[] param);

        /** */
        Employee[] testEmployees(Employee[] emps);

        /** */
        Account[] testAccounts();

        /** */
        User[] testUsers();

        /** */
        ICollection testDepartments(ICollection deps);

        /** */
        IDictionary testMap(IDictionary<Key, Value> dict);

        /** */
        void testDateArray(DateTime?[] dates);

        /** */
        DateTime testDate(DateTime date);

        /** */
        void testUTCDateFromCache();

        /** */
        void testLocalDateFromCache();

        /** */
        void testException(string exceptionClass);

        /** */
        object testRoundtrip(object x);

        /** */
        void sleep(long delayMs);

        /** */
        void putValsForCache();

        /** */
        void startReceiveMessage();

        /** */
        bool testMessagesReceived();

        /** */
        void testSendMessage();

        /** */
        object contextAttribute(string name);
    }

    /// <summary>
    /// Interface for the methods that are available only on Java side.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public interface IJavaOnlyService : IJavaService
    {
        /// <summary>
        /// Returns number of measured by service metrics invocations of all service's methods or of its certain method.
        /// </summary>
        int testNumberOfInvocations(string svcName, string histName = null);
    }
}
