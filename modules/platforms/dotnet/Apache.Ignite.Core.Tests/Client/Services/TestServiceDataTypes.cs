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
    using System.Linq;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service for testing various data types passing.
    /// </summary>
    public class TestServiceDataTypes : ITestServiceDataTypes, IService
    {
        /** <inheritdoc /> */
        public byte GetByte(byte x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public byte[] GetByteArray(byte[] x)
        {
            return x.Select(b => ++b).ToArray();
        }

        /** <inheritdoc /> */
        public sbyte GetSbyte(sbyte x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public sbyte[] GetSbyteArray(sbyte[] x)
        {
            return x.Select(b => ++b).ToArray();
        }

        /** <inheritdoc /> */
        public char GetChar(char x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public char[] GetCharArray(char[] x)
        {
            return x.Select(b => ++b).ToArray();
        }

        /** <inheritdoc /> */
        public short GetShort(short x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public short[] GetShortArray(short[] x)
        {
            return x.Select(s => ++s).ToArray();
        }

        /** <inheritdoc /> */
        public ushort GetUShort(ushort x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public ushort[] GetUShortArray(ushort[] x)
        {
            return x.Select(s => ++s).ToArray();
        }

        /** <inheritdoc /> */
        public int GetInt(int x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public int[] GetIntArray(int[] x)
        {
            return x.Select(s => ++s).ToArray();
        }

        /** <inheritdoc /> */
        public uint GetUInt(uint x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public uint[] GetUIntArray(uint[] x)
        {
            return x.Select(s => ++s).ToArray();
        }

        /** <inheritdoc /> */
        public long GetLong(long x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public long[] GetLongArray(long[] x)
        {
            return x.Select(s => ++s).ToArray();
        }

        /** <inheritdoc /> */
        public ulong GetULong(ulong x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public ulong[] GetULongArray(ulong[] x)
        {
            return x.Select(b => ++b).ToArray();
        }

        /** <inheritdoc /> */
        public Guid GetGuid(Guid x)
        {
            return x;
        }

        /** <inheritdoc /> */
        public Guid[] GetGuidArray(Guid[] x)
        {
            return x.ToArray();
        }

        /** <inheritdoc /> */
        public DateTime GetDateTime(DateTime x)
        {
            return x.AddDays(1);
        }

        /** <inheritdoc /> */
        public DateTime[] GetDateTimeArray(DateTime[] x)
        {
            return x.Select(d => d.AddDays(1)).ToArray();
        }

        /** <inheritdoc /> */
        public List<DateTime> GetDateTimeList(ICollection<DateTime> x)
        {
            return x.Select(d => d.AddDays(1)).ToList();
        }

        /** <inheritdoc /> */
        public TimeSpan GetTimeSpan(TimeSpan x)
        {
            return x.Add(TimeSpan.FromMinutes(1));
        }

        /** <inheritdoc /> */
        public TimeSpan[] GetTimeSpanArray(TimeSpan[] x)
        {
            return x.Select(b => b.Add(TimeSpan.FromMinutes(1))).ToArray();
        }

        /** <inheritdoc /> */
        public bool GetBool(bool x)
        {
            return !x;
        }

        /** <inheritdoc /> */
        public bool[] GetBoolArray(bool[] x)
        {
            return x.Select(b => !b).ToArray();
        }

        /** <inheritdoc /> */
        public float GetFloat(float x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public float[] GetFloatArray(float[] x)
        {
            return x.Select(b => ++b).ToArray();
        }

        /** <inheritdoc /> */
        public double GetDouble(double x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public double[] GetDoubleArray(double[] x)
        {
            return x.Select(b => ++b).ToArray();
        }

        /** <inheritdoc /> */
        public decimal GetDecimal(decimal x)
        {
            return ++x;
        }

        /** <inheritdoc /> */
        public decimal[] GetDecimalArray(decimal[] x)
        {
            return x.Select(b => ++b).ToArray();
        }

        /** <inheritdoc /> */
        public string GetString(string x)
        {
            return x.ToLowerInvariant();
        }

        /** <inheritdoc /> */
        public string[] GetStringArray(string[] x)
        {
            return x.Select(b => b.ToLowerInvariant()).ToArray();
        }

        /** <inheritdoc /> */
        public void Init(IServiceContext context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Execute(IServiceContext context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Cancel(IServiceContext context)
        {
            // No-op.
        }
    }
}
