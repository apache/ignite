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
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests numerics.
        /// </summary>
        [Test]
        public void TestNumerics()
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, Numerics>(new CacheConfiguration("numerics", 
                    new QueryEntity(typeof(int), typeof(Numerics)))
                {
                    SqlEscapeAll = GetSqlEscapeAll()
                });

            for (var i = 0; i < 100; i++)
                cache[i] = new Numerics(((double)i - 50) / 3);

            var query = cache.AsCacheQueryable().Select(x => x.Value);

            var bytes = query.Select(x => x.Byte);
            var sbytes = query.Select(x => x.Sbyte);
            var shorts = query.Select(x => x.Short);
            var ushorts = query.Select(x => x.Ushort);
            var ints = query.Select(x => x.Int);
            var uints = query.Select(x => x.Uint);
            var longs = query.Select(x => x.Long);
            var ulongs = query.Select(x => x.Ulong);
            var doubles = query.Select(x => x.Double);
            var decimals = query.Select(x => x.Decimal);
            var floats = query.Select(x => x.Float);

            CheckFunc(x => Math.Abs(x), doubles);
            CheckFunc(x => Math.Abs((sbyte)x), bytes);
            CheckFunc(x => Math.Abs(x), sbytes);
            CheckFunc(x => Math.Abs(x), shorts);
            CheckFunc(x => Math.Abs((short)x), ushorts);
            CheckFunc(x => Math.Abs(x), ints);
            CheckFunc(x => Math.Abs((int)x), uints);
            CheckFunc(x => Math.Abs(x), longs);
            CheckFunc(x => Math.Abs((long)x), ulongs);
            CheckFunc(x => Math.Abs(x), decimals);
            CheckFunc(x => Math.Abs(x), floats);

            CheckFunc(x => Math.Acos(x), doubles);
            CheckFunc(x => Math.Asin(x), doubles);
            CheckFunc(x => Math.Atan(x), doubles);
            CheckFunc(x => Math.Atan2(x, 0.5), doubles);

            CheckFunc(x => Math.Ceiling(x), doubles);
            CheckFunc(x => Math.Ceiling(x), decimals);

            CheckFunc(x => Math.Cos(x), doubles);
            CheckFunc(x => Math.Cosh(x), doubles);
            CheckFunc(x => Math.Exp(x), doubles);

            CheckFunc(x => Math.Floor(x), doubles);
            CheckFunc(x => Math.Floor(x), decimals);

            CheckFunc(x => Math.Log(x), doubles);
            CheckFunc(x => Math.Log10(x), doubles);

            CheckFunc(x => Math.Pow(x, 3.7), doubles);

            CheckFunc(x => Math.Round(x), doubles);
            CheckFunc(x => Math.Round(x, 3), doubles);
            CheckFunc(x => Math.Round(x), decimals);
            CheckFunc(x => Math.Round(x, 3), decimals);

            CheckFunc(x => Math.Sign(x), doubles);
            CheckFunc(x => Math.Sign(x), decimals);
            CheckFunc(x => Math.Sign(x), floats);
            CheckFunc(x => Math.Sign(x), ints);
            CheckFunc(x => Math.Sign(x), longs);
            CheckFunc(x => Math.Sign(x), shorts);
            CheckFunc(x => Math.Sign(x), sbytes);

            CheckFunc(x => Math.Sin(x), doubles);
            CheckFunc(x => Math.Sinh(x), doubles);
            CheckFunc(x => Math.Sqrt(x), doubles);
            CheckFunc(x => Math.Tan(x), doubles);
            CheckFunc(x => Math.Tanh(x), doubles);

            CheckFunc(x => Math.Truncate(x), doubles);
            CheckFunc(x => Math.Truncate(x), decimals);

            // Operators
            CheckFunc(x => x * 7, doubles);
            CheckFunc(x => x / 7, doubles);
            CheckFunc(x => x % 7, doubles);
            CheckFunc(x => x + 7, doubles);
            CheckFunc(x => x - 7, doubles);
        }
    }
}