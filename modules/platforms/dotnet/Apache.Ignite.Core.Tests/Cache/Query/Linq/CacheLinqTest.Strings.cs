﻿/*
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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests strings.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "StringCompareIsCultureSpecific.2", Justification = "SQL")]
        [SuppressMessage("ReSharper", "StringCompareIsCultureSpecific.3", Justification = "SQL")]
        public void TestStrings()
        {
            var strings = GetSecondPersonCache().AsCacheQueryable().Select(x => x.Value.Name);

            CheckFunc(x => x.PadLeft(20), strings);
            CheckFunc(x => x.PadLeft(20, 'l'), strings);
            CheckFunc(x => x.PadRight(20), strings);
            CheckFunc(x => x.PadRight(20, 'r'), strings);

            CheckFunc(x => x.ToLower(), strings);
            CheckFunc(x => x.ToUpper(), strings);
            CheckFunc(x => x.StartsWith("Person_9"), strings);
            CheckFunc(x => x.EndsWith("7"), strings);
            CheckFunc(x => x.Contains("son_3"), strings);
            CheckFunc(x => x.Length, strings);

            CheckFunc(x => x.IndexOf("9"), strings);
            CheckFunc(x => x.IndexOf("7", 4), strings);

            CheckFunc(x => x.Substring(4), strings);
            CheckFunc(x => x.Substring(4, 5), strings);

            CheckFunc(x => x.Trim(), strings);

            CheckFunc(x => x.Trim('P'), strings);
            var toTrim = new[] { 'P' };
            CheckFunc(x => x.Trim(toTrim), strings);
            CheckFunc(x => x.Trim(new List<char> { 'P' }.ToArray()), strings);
            CheckFunc(x => x.Trim('3'), strings);
            CheckFunc(x => x.TrimStart('P'), strings);
            CheckFunc(x => x.TrimStart(toTrim), strings);
            CheckFunc(x => x.TrimStart('3'), strings);
            Assert.Throws<NotSupportedException>(() => CheckFunc(x => x.TrimStart('P', 'e'), strings));
            CheckFunc(x => x.TrimEnd('P'), strings);
            CheckFunc(x => x.TrimEnd(toTrim), strings);
            CheckFunc(x => x.TrimEnd('3'), strings);
            var toTrimFails = new[] { 'P', 'c' };
            Assert.Throws<NotSupportedException>(() => CheckFunc(x => x.Trim(toTrimFails), strings));
            Assert.Throws<NotSupportedException>(() => CheckFunc(x => x.TrimStart(toTrimFails), strings));
            Assert.Throws<NotSupportedException>(() => CheckFunc(x => x.TrimEnd(toTrimFails), strings));

            CheckFunc(x => Regex.Replace(x, @"son.\d", "kele!"), strings);
            CheckFunc(x => Regex.Replace(x, @"son.\d", "kele!", RegexOptions.None), strings);
            CheckFunc(x => Regex.Replace(x, @"person.\d", "akele!", RegexOptions.IgnoreCase), strings);
            CheckFunc(x => Regex.Replace(x, @"person.\d", "akele!", RegexOptions.Multiline), strings);
            CheckFunc(x => Regex.Replace(x, @"person.\d", "akele!", RegexOptions.IgnoreCase | RegexOptions.Multiline),
                strings);
            var notSupportedException = Assert.Throws<NotSupportedException>(() => CheckFunc(x =>
                Regex.IsMatch(x, @"^person\d", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), strings));
            Assert.AreEqual("RegexOptions.CultureInvariant is not supported", notSupportedException.Message);

            CheckFunc(x => Regex.IsMatch(x, @"^Person_9\d"), strings);
            CheckFunc(x => Regex.IsMatch(x, @"^person_9\d", RegexOptions.None), strings);
            CheckFunc(x => Regex.IsMatch(x, @"^person_9\d", RegexOptions.IgnoreCase), strings);
            CheckFunc(x => Regex.IsMatch(x, @"^Person_9\d", RegexOptions.Multiline), strings);
            CheckFunc(x => Regex.IsMatch(x, @"^person_9\d", RegexOptions.IgnoreCase | RegexOptions.Multiline), strings);
            notSupportedException = Assert.Throws<NotSupportedException>(() => CheckFunc(x =>
                Regex.IsMatch(x, @"^person_9\d",RegexOptions.IgnoreCase | RegexOptions.CultureInvariant), strings));
            Assert.AreEqual("RegexOptions.CultureInvariant is not supported", notSupportedException.Message);

            CheckFunc(x => x.Replace("son", ""), strings);
            CheckFunc(x => x.Replace("son", "kele"), strings);

            // Concat
            CheckFunc(x => x + x, strings);

            // String + int
            CheckFunc(x => x + 10, strings);

            // string.Compare(string strA, string strB)
            CheckWhereFunc(x => string.Compare(x, "Person_1300") < 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300") > 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300") == 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300") <= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300") >= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) < 0, x => string.Compare(x, "person_1300") < 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) > 0, x => string.Compare(x, "person_1300") > 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300") == 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) <= 0, x => string.Compare(x, "person_1300") <= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) >= 0, x => string.Compare(x, "person_1300") >= 0, strings);
            CheckWhereFunc(x => string.Compare(x, null) == 0, strings);
            CheckWhereFunc(x => string.Compare(x, null) < 0, strings);
            CheckWhereFunc(x => string.Compare(x, null) > 0, strings);
            CheckWhereFunc(x => string.Compare(null, x) == 0, strings);
            CheckWhereFunc(x => string.Compare(null, x) < 0, strings);
            CheckWhereFunc(x => string.Compare(null, x) > 0, strings);
            CheckWhereFunc(x => string.Compare(null, null) == 0, strings);
            CheckWhereFunc(x => string.Compare(null, null) < 0, strings);
            CheckWhereFunc(x => string.Compare(null, null) > 0, strings);

            // string.Compare(string strA, string strB, true)
            CheckWhereFunc(x => string.Compare(x, "Person_1300", true) < 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", true) > 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", true) == 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", true) <= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", true) >= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", true) < 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", true) > 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", true) == 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", true) <= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", true) >= 0, strings);
            CheckWhereFunc(x => string.Compare(x, null, true) == 0, strings);
            CheckWhereFunc(x => string.Compare(x, null, true) < 0, strings);
            CheckWhereFunc(x => string.Compare(x, null, true) > 0, strings);
            CheckWhereFunc(x => string.Compare(null, x, true) == 0, strings);
            CheckWhereFunc(x => string.Compare(null, x, true) < 0, strings);
            CheckWhereFunc(x => string.Compare(null, x, true) > 0, strings);
            CheckWhereFunc(x => string.Compare(null, null, true) == 0, strings);
            CheckWhereFunc(x => string.Compare(null, null, true) < 0, strings);
            CheckWhereFunc(x => string.Compare(null, null, true) > 0, strings);

            // string.Compare(string strA, string strB, false)
            CheckWhereFunc(x => string.Compare(x, "Person_1300", false) < 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", false) > 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", false) == 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", false) <= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "Person_1300", false) >= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) < 0, x => string.Compare(x, "person_1300", false) < 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) > 0, x => string.Compare(x, "person_1300", false) > 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", false) == 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) <= 0, x => string.Compare(x, "person_1300", false) <= 0, strings);
            CheckWhereFunc(x => string.Compare(x, "person_1300", StringComparison.Ordinal) >= 0, x => string.Compare(x, "person_1300", false) >= 0, strings);
            CheckWhereFunc(x => string.Compare(x, null, false) == 0, strings);
            CheckWhereFunc(x => string.Compare(x, null, false) < 0, strings);
            CheckWhereFunc(x => string.Compare(x, null, false) > 0, strings);
            CheckWhereFunc(x => string.Compare(null, x, false) == 0, strings);
            CheckWhereFunc(x => string.Compare(null, x, false) < 0, strings);
            CheckWhereFunc(x => string.Compare(null, x, false) > 0, strings);
            CheckWhereFunc(x => string.Compare(null, null, false) == 0, strings);
            CheckWhereFunc(x => string.Compare(null, null, false) < 0, strings);
            CheckWhereFunc(x => string.Compare(null, null, false) > 0, strings);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed (force LINQ evaluation)
            Assert.Throws<NotSupportedException>(
                () => strings.Where(x => string.Compare(x, "person_1300", x.StartsWith("Person")) == 0).ToArray(),
                "Parameter 'ignoreCase' from 'string.Compare' method should be specified as a constant expression");
        }
    }
}
