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

namespace Apache.Ignite.Core.Tests
{
    using System.Configuration;
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="IgniteConfigurationSection"/>.
    /// </summary>
    public class IgniteConfigurationSectionTest
    {
        [Test]
        public void TestRead()
        {
            var section = (IgniteConfigurationSection) ConfigurationManager.GetSection("igniteConfiguration");

            Assert.AreEqual("myGrid1", section.IgniteConfiguration.GridName);
            Assert.AreEqual("cacheName", section.IgniteConfiguration.CacheConfiguration.Single().Name);
        }

        [Test]
        public void TestIgniteStart()
        {
            using (var ignite = Ignition.Start("igniteConfiguration"))
            {
                
            }

            using (var ignite = Ignition.Start("igniteConfiguration2"))
            {
                
            }
        }
    }
}
