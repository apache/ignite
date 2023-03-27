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

namespace Apache.Ignite.Core.Tests.Client.Services
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Service interface for testing various data types passing.
    /// </summary>
    public interface ITestServiceDataTypes
    {
        /** */
        byte GetByte(byte x);

        /** */
        byte[] GetByteArray(byte[] x);

        /** */
        sbyte GetSbyte(sbyte x);

        /** */
        sbyte[] GetSbyteArray(sbyte[] x);

        /** */
        char GetChar(char x);

        /** */
        char[] GetCharArray(char[] x);

        /** */
        short GetShort(short x);

        /** */
        short[] GetShortArray(short[] x);

        /** */
        ushort GetUShort(ushort x);

        /** */
        ushort[] GetUShortArray(ushort[] x);

        /** */
        int GetInt(int x);

        /** */
        int[] GetIntArray(int[] x);

        /** */
        uint GetUInt(uint x);

        /** */
        uint[] GetUIntArray(uint[] x);

        /** */
        long GetLong(long x);

        /** */
        long[] GetLongArray(long[] x);

        /** */
        ulong GetULong(ulong x);

        /** */
        ulong[] GetULongArray(ulong[] x);

        /** */
        Guid GetGuid(Guid x);

        /** */
        Guid[] GetGuidArray(Guid[] x);

        /** */
        DateTime GetDateTime(DateTime x);

        /** */
        DateTime[] GetDateTimeArray(DateTime[] x);

        /** */
        List<DateTime> GetDateTimeList(ICollection<DateTime> x);

        /** */
        TimeSpan GetTimeSpan(TimeSpan x);

        /** */
        TimeSpan[] GetTimeSpanArray(TimeSpan[] x);

        /** */
        bool GetBool(bool x);

        /** */
        bool[] GetBoolArray(bool[] x);

        /** */
        float GetFloat(float x);

        /** */
        float[] GetFloatArray(float[] x);

        /** */
        double GetDouble(double x);

        /** */
        double[] GetDoubleArray(double[] x);

        /** */
        decimal GetDecimal(decimal x);

        /** */
        decimal[] GetDecimalArray(decimal[] x);

        /** */
        string GetString(string x);

        /** */
        string[] GetStringArray(string[] x);
    }
}
