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
    using System;
    using System.Configuration;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="IgniteConfigurationSection"/>.
    /// </summary>
    public class IgniteConfigurationSectionTest
    {
        /// <summary>
        /// Tests the read.
        /// </summary>
        [Test]
        public void TestRead()
        {
            var section = (IgniteConfigurationSection) ConfigurationManager.GetSection("igniteConfiguration");

            Assert.AreEqual("myGrid1", section.IgniteConfiguration.GridName);
            Assert.AreEqual("cacheName", section.IgniteConfiguration.CacheConfiguration.Single().Name);
        }

        /// <summary>
        /// Tests the ignite start.
        /// </summary>
        [Test]
        public void TestIgniteStart()
        {
            Environment.SetEnvironmentVariable(Classpath.EnvIgniteNativeTestClasspath, "true");

            using (var ignite = Ignition.StartFromApplicationConfiguration("igniteConfiguration"))
            {
                Assert.AreEqual("myGrid1", ignite.Name);
                Assert.IsNotNull(ignite.GetCache<int, int>("cacheName"));
            }

            using (var ignite = Ignition.StartFromApplicationConfiguration("igniteConfiguration2"))
            {
                Assert.AreEqual("myGrid2", ignite.Name);
                Assert.IsNotNull(ignite.GetCache<int, int>("cacheName2"));
            }

            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Assert.IsTrue(ignite.Name.StartsWith("myGrid"));
            }

            using (var ignite = Ignition.StartFromApplicationConfiguration(
                "igniteConfiguration3", "custom_app.config"))
            {
                Assert.AreEqual("myGrid3", ignite.Name);
            }
        }

        /// <summary>
        /// Tests the ignite start error.
        /// </summary>
        [Test]
        public void TestIgniteStartError()
        {
            var ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfiguration111"));

            Assert.AreEqual("Could not find IgniteConfigurationSection with name 'igniteConfiguration111'", 
                ex.Message);


            ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfiguration", "somefile"));

            Assert.AreEqual("Specified config file does not exist: somefile", ex.Message);


            ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfiguration", "custom_app.config"));

            Assert.AreEqual("Could not find IgniteConfigurationSection with name 'igniteConfiguration' " +
                            "in file 'custom_app.config'", ex.Message);
        }
    }
}
