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

namespace Apache.Ignite.Core.Tests
{
    using System.IO;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests IgniteManager class.
    /// </summary>
    public class IgniteManagerTest
    {
        /// <summary>
        /// Tests home dir resolver.
        /// </summary>
        [Test]
        public void TestIgniteHome()
        {
            using (EnvVar.Set(IgniteHome.EnvIgniteHome, null))
            {
                var home = IgniteHome.Resolve();
                Assert.IsTrue(Directory.Exists(home));

                // Invalid home.
                const string invalidHome = @"c:\foo\bar";
                var ex = Assert.Throws<IgniteException>(() => IgniteHome.Resolve(invalidHome));
                Assert.AreEqual(string.Format(
                    "IgniteConfiguration.IgniteHome is not valid: '{0}'", invalidHome), ex.Message);
            }
        }
    }
}
