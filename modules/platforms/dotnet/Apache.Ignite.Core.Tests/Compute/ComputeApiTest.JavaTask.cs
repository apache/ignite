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

// ReSharper disable SpecifyACultureInStringConversionExplicitly
namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Compute tests with Java tasks.
    /// </summary>
    public partial class ComputeApiTest
    {
        /** Echo task name. */
        public const string EchoTask = "org.apache.ignite.platform.PlatformComputeEchoTask";

        /** Binary argument task name. */
        public const string BinaryArgTask = "org.apache.ignite.platform.PlatformComputeBinarizableArgTask";

        /** Broadcast task name. */
        public const string BroadcastTask = "org.apache.ignite.platform.PlatformComputeBroadcastTask";

        /** Decimal task name. */
        private const string DecimalTask = "org.apache.ignite.platform.PlatformComputeDecimalTask";

        /** Echo type: null. */
        private const int EchoTypeNull = 0;

        /** Echo type: byte. */
        private const int EchoTypeByte = 1;

        /** Echo type: bool. */
        private const int EchoTypeBool = 2;

        /** Echo type: short. */
        private const int EchoTypeShort = 3;

        /** Echo type: char. */
        private const int EchoTypeChar = 4;

        /** Echo type: int. */
        public const int EchoTypeInt = 5;

        /** Echo type: long. */
        private const int EchoTypeLong = 6;

        /** Echo type: float. */
        private const int EchoTypeFloat = 7;

        /** Echo type: double. */
        private const int EchoTypeDouble = 8;

        /** Echo type: array. */
        private const int EchoTypeArray = 9;

        /** Echo type: collection. */
        private const int EchoTypeCollection = 10;

        /** Echo type: map. */
        private const int EchoTypeMap = 11;

        /** Echo type: binarizable. */
        public const int EchoTypeBinarizable = 12;

        /** Echo type: binary (Java only). */
        public const int EchoTypeBinarizableJava = 13;

        /** Type: object array. */
        private const int EchoTypeObjArray = 14;

        /** Type: binary object array. */
        private const int EchoTypeBinarizableArray = 15;

        /** Type: enum. */
        private const int EchoTypeEnum = 16;

        /** Type: enum array. */
        private const int EchoTypeEnumArray = 17;

        /** Type: enum field. */
        private const int EchoTypeEnumField = 18;

        /** Type: affinity key. */
        public const int EchoTypeAffinityKey = 19;

        /** Type: enum from cache. */
        private const int EchoTypeEnumFromCache = 20;

        /** Type: enum array from cache. */
        private const int EchoTypeEnumArrayFromCache = 21;

        /** Echo type: IgniteUuid. */
        private const int EchoTypeIgniteUuid = 22;

        /** Echo type: binary enum (created with builder). */
        private const int EchoTypeBinaryEnum = 23;

        /// <summary>
        /// Test echo with decimals.
        /// </summary>
        [Test]
        public void TestEchoDecimal()
        {
            decimal val;

            Assert.AreEqual(val = decimal.Zero, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("65536"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("4294967296"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("281474976710656"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("18446744073709551616"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.MaxValue, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MaxValue - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("11,12"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-11,12"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            // Test echo with overflow.
            var ex = Assert.Throws<BinaryObjectException>(() => _grid1.GetCompute()
                .ExecuteJavaTask<object>(DecimalTask, new object[] { null, decimal.MaxValue.ToString() + 1 }));

            Assert.AreEqual("Decimal magnitude overflow (must be less than 96 bits): 104", ex.Message);

            // Negative scale. 1E+1 parses to "1 scale -1" on Java side.
            ex = Assert.Throws<BinaryObjectException>(() => _grid1.GetCompute()
                .ExecuteJavaTask<object>(DecimalTask, new object[] { null, "1E+1" }));

            Assert.AreEqual("Decimal value scale overflow (must be between 0 and 28): -1", ex.Message);
        }

        /// <summary>
        /// Test echo task returning null.
        /// </summary>
        [Test]
        public void TestEchoTaskNull()
        {
            Assert.IsNull(_grid1.GetCompute().ExecuteJavaTask<object>(EchoTask, EchoTypeNull));
        }

        /// <summary>
        /// Test echo task returning various primitives.
        /// </summary>
        [Test]
        public void TestEchoTaskPrimitives()
        {
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<byte>(EchoTask, EchoTypeByte));
            Assert.AreEqual(true, _grid1.GetCompute().ExecuteJavaTask<bool>(EchoTask, EchoTypeBool));
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<short>(EchoTask, EchoTypeShort));
            Assert.AreEqual((char)1, _grid1.GetCompute().ExecuteJavaTask<char>(EchoTask, EchoTypeChar));
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<int>(EchoTask, EchoTypeInt));
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<long>(EchoTask, EchoTypeLong));
            Assert.AreEqual((float)1, _grid1.GetCompute().ExecuteJavaTask<float>(EchoTask, EchoTypeFloat));
            Assert.AreEqual((double)1, _grid1.GetCompute().ExecuteJavaTask<double>(EchoTask, EchoTypeDouble));
        }

        /// <summary>
        /// Test echo task returning compound types.
        /// </summary>
        [Test]
        public void TestEchoTaskCompound()
        {
            int[] res1 = _grid1.GetCompute().ExecuteJavaTask<int[]>(EchoTask, EchoTypeArray);

            Assert.AreEqual(1, res1.Length);
            Assert.AreEqual(1, res1[0]);

            var res2 = _grid1.GetCompute().ExecuteJavaTask<IList>(EchoTask, EchoTypeCollection);

            Assert.AreEqual(1, res2.Count);
            Assert.AreEqual(1, res2[0]);

            var res3 = _grid1.GetCompute().ExecuteJavaTask<IDictionary>(EchoTask, EchoTypeMap);

            Assert.AreEqual(1, res3.Count);
            Assert.AreEqual(1, res3[1]);
        }

        /// <summary>
        /// Test echo task returning binary object.
        /// </summary>
        [Test]
        public void TestEchoTaskBinarizable()
        {
            var values = new[] { int.MinValue, int.MaxValue, 0, 1, -1, byte.MaxValue, byte.MinValue };
            var cache = _grid1.GetCache<int, int>(DefaultCacheName);
            var compute = _grid1.GetCompute();

            foreach (var val in values)
            {
                cache[EchoTypeBinarizable] = val;

                var res = compute.ExecuteJavaTask<PlatformComputeBinarizable>(EchoTask, EchoTypeBinarizable);
                Assert.AreEqual(val, res.Field);

                // Binary mode.
                var binRes = compute.WithKeepBinary().ExecuteJavaTask<IBinaryObject>(EchoTask, EchoTypeBinarizable);

                Assert.AreEqual(val, binRes.GetField<long>("Field"));

#if !NETCOREAPP
                var dotNetBin = _grid1.GetBinary().ToBinary<BinaryObject>(res);

                Assert.AreEqual(dotNetBin.Header.HashCode, ((BinaryObject)binRes).Header.HashCode);

                Func<BinaryObject, byte[]> getData = bo => bo.Data.Skip(bo.Offset).Take(bo.Header.Length).ToArray();
                Assert.AreEqual(getData(dotNetBin), getData((BinaryObject)binRes));
#endif
            }
        }

        /// <summary>
        /// Test echo task returning binary object with no corresponding class definition.
        /// </summary>
        [Test]
        public void TestEchoTaskBinarizableNoClass()
        {
            ICompute compute = _grid1.GetCompute();

            compute.WithKeepBinary();

            IBinaryObject res = compute.ExecuteJavaTask<IBinaryObject>(EchoTask, EchoTypeBinarizableJava);

            Assert.AreEqual(1, res.GetField<int>("field"));

            // This call must fail because "keepBinary" flag is reset.
            var ex = Assert.Throws<BinaryObjectException>(() =>
            {
                compute.ExecuteJavaTask<IBinaryObject>(EchoTask, EchoTypeBinarizableJava);
            });

            Assert.AreEqual(
                "Failed to resolve class name [platformId=1, platform=.NET, typeId=2009791293]", ex.Message);
        }

        /// <summary>
        /// Tests the echo task returning object array.
        /// </summary>
        [Test]
        public void TestEchoTaskObjectArray()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<string[]>(EchoTask, EchoTypeObjArray);

            Assert.AreEqual(new[] { "foo", "bar", "baz" }, res);
        }

        /// <summary>
        /// Tests the echo task returning binary array.
        /// </summary>
        [Test]
        public void TestEchoTaskBinarizableArray()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<object[]>(EchoTask, EchoTypeBinarizableArray);

            Assert.AreEqual(3, res.Length);

            for (var i = 0; i < res.Length; i++)
                Assert.AreEqual(i + 1, ((PlatformComputeBinarizable)res[i]).Field);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnum()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum>(EchoTask, EchoTypeEnum);

            Assert.AreEqual(PlatformComputeEnum.Bar, res);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskBinaryEnum()
        {
            var res = _grid1.GetCompute().WithKeepBinary()
                .ExecuteJavaTask<IBinaryObject>(EchoTask, EchoTypeBinaryEnum);

            Assert.AreEqual("JavaFoo", res.EnumName);
            Assert.AreEqual(1, res.EnumValue);

            var binType = res.GetBinaryType();

            Assert.IsTrue(binType.IsEnum);
            Assert.AreEqual("JavaDynEnum", binType.TypeName);

            var vals = binType.GetEnumValues().OrderBy(x => x.EnumValue).ToArray();
            Assert.AreEqual(new[] { 1, 2 }, vals.Select(x => x.EnumValue));
            Assert.AreEqual(new[] { "JavaFoo", "JavaBar" }, vals.Select(x => x.EnumName));
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumFromCache()
        {
            var cache = _grid1.GetCache<int, PlatformComputeEnum>(DefaultCacheName);

            foreach (PlatformComputeEnum val in Enum.GetValues(typeof(PlatformComputeEnum)))
            {
                cache[EchoTypeEnumFromCache] = val;

                var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum>(EchoTask, EchoTypeEnumFromCache);

                Assert.AreEqual(val, res);
            }
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumArray()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum[]>(EchoTask, EchoTypeEnumArray);

            Assert.AreEqual(new[]
            {
                PlatformComputeEnum.Bar,
                PlatformComputeEnum.Baz,
                PlatformComputeEnum.Foo
            }, res);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumArrayFromCache()
        {
            var cache = _grid1.GetCache<int, PlatformComputeEnum[]>(DefaultCacheName);

            foreach (var val in new[]
            {
                new[] {PlatformComputeEnum.Bar, PlatformComputeEnum.Baz, PlatformComputeEnum.Foo },
                new[] {PlatformComputeEnum.Foo, PlatformComputeEnum.Baz},
                new[] {PlatformComputeEnum.Bar}
            })
            {
                cache[EchoTypeEnumArrayFromCache] = val;

                var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum[]>(
                    EchoTask, EchoTypeEnumArrayFromCache);

                Assert.AreEqual(val, res);
            }
        }

        /// <summary>
        /// Tests the echo task reading enum from a binary object field.
        /// Ensures that Java can understand enums written by .NET.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumField()
        {
            var enumVal = PlatformComputeEnum.Baz;

            _grid1.GetCache<int, InteropComputeEnumFieldTest>(DefaultCacheName)
                .Put(EchoTypeEnumField, new InteropComputeEnumFieldTest { InteropEnum = enumVal });

            var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum>(EchoTask, EchoTypeEnumField);

            var enumMeta = _grid1.GetBinary().GetBinaryType(typeof(PlatformComputeEnum));

            Assert.IsTrue(enumMeta.IsEnum);
            Assert.AreEqual(enumMeta.TypeName, typeof(PlatformComputeEnum).Name);
            Assert.AreEqual(0, enumMeta.Fields.Count);

            Assert.AreEqual(enumVal, res);
        }

        /// <summary>
        /// Tests that IgniteGuid in .NET maps to IgniteUuid in Java.
        /// </summary>
        [Test]
        public void TestEchoTaskIgniteUuid()
        {
            var guid = Guid.NewGuid();

            _grid1.GetCache<int, object>(DefaultCacheName)[EchoTypeIgniteUuid] = new IgniteGuid(guid, 25);

            var res = _grid1.GetCompute().ExecuteJavaTask<IgniteGuid>(EchoTask, EchoTypeIgniteUuid);

            Assert.AreEqual(guid, res.GlobalId);
            Assert.AreEqual(25, res.LocalId);
        }

        /// <summary>
        /// Test for binary argument in Java.
        /// </summary>
        [Test]
        public void TestBinarizableArgTask()
        {
            ICompute compute = _grid1.GetCompute();

            compute.WithKeepBinary();

            PlatformComputeNetBinarizable arg = new PlatformComputeNetBinarizable { Field = 100 };

            int res = compute.ExecuteJavaTask<int>(BinaryArgTask, arg);

            Assert.AreEqual(arg.Field, res);
        }

        /// <summary>
        /// Test running broadcast task.
        /// </summary>
        [Test]
        public void TestBroadcastTask([Values(false, true)] bool isAsync)
        {
            var execTask =
                isAsync
                    ? (Func<ICompute, List<Guid>>)(
                        c => c.ExecuteJavaTaskAsync<ICollection>(BroadcastTask, null).Result.OfType<Guid>().ToList())
                    : c => c.ExecuteJavaTask<ICollection>(BroadcastTask, null).OfType<Guid>().ToList();

            var res = execTask(_grid1.GetCompute());

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(0)).GetNodes().Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(1)).GetNodes().Count);

            var prj = _grid1.GetCluster().ForPredicate(node => res.Take(2).Contains(node.Id));

            Assert.AreEqual(2, prj.GetNodes().Count);

            var filteredRes = execTask(prj.GetCompute());

            Assert.AreEqual(2, filteredRes.Count);
            Assert.IsTrue(filteredRes.Contains(res.ElementAt(0)));
            Assert.IsTrue(filteredRes.Contains(res.ElementAt(1)));
        }

        /// <summary>
        /// Test "withNoFailover" feature.
        /// </summary>
        [Test]
        public void TestWithNoFailover()
        {
            var res = _grid1.GetCompute().WithNoFailover().ExecuteJavaTask<ICollection>(BroadcastTask, null)
                .OfType<Guid>().ToList();

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(0)).GetNodes().Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(1)).GetNodes().Count);
        }

        /// <summary>
        /// Test "withTimeout" feature.
        /// </summary>
        [Test]
        public void TestWithTimeout()
        {
            var res = _grid1.GetCompute().WithTimeout(1000).ExecuteJavaTask<ICollection>(BroadcastTask, null)
                .OfType<Guid>().ToList();

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(0)).GetNodes().Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(1)).GetNodes().Count);
        }
    }
}
