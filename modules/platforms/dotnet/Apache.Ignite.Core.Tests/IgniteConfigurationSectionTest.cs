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
            var section = (IgniteConfigurationSection) ConfigurationManager.GetSection(
                Ignition.ConfigurationSectionName);

            Assert.AreEqual("myGrid1", section.IgniteConfiguration.IgniteInstanceName);
            Assert.AreEqual("cacheName", section.IgniteConfiguration.CacheConfiguration.Single().Name);
        }

        /// <summary>
        /// Tests the ignite start.
        /// </summary>
        [Test]
        public void TestIgniteStart()
        {
            using (EnvVar.Set(Classpath.EnvIgniteNativeTestClasspath, bool.TrueString))
            {
                using (var ignite = Ignition.StartFromApplicationConfiguration(Ignition.ConfigurationSectionName))
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
                    Assert.AreEqual("myGrid1", ignite.Name);
                }

                using (var ignite = Ignition.StartFromApplicationConfiguration(
                    "igniteConfiguration3", "custom_app.config"))
                {
                    Assert.AreEqual("myGrid3", ignite.Name);
                }
            }
        }

        /// <summary>
        /// Tests the ignite start error.
        /// </summary>
        [Test]
        public void TestIgniteStartError()
        {
            // Missing section in default file.
            var ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfiguration111"));

            Assert.AreEqual("Could not find IgniteConfigurationSection with name 'igniteConfiguration111'", 
                ex.Message);


            // Missing section body.
            ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfigurationMissing"));

            Assert.AreEqual("IgniteConfigurationSection with name 'igniteConfigurationMissing' " +
                            "is defined in <configSections>, but not present in configuration.", ex.Message);


            // Missing custom file.
            ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfiguration", "somefile"));

            Assert.AreEqual("Specified config file does not exist: somefile", ex.Message);


            // Missing section in custom file.
            ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfiguration", "custom_app.config"));

            Assert.AreEqual("Could not find IgniteConfigurationSection with name 'igniteConfiguration' " +
                            "in file 'custom_app.config'", ex.Message);
            
            
            // Missing section body in custom file.
            ex = Assert.Throws<ConfigurationErrorsException>(() =>
                Ignition.StartFromApplicationConfiguration("igniteConfigurationMissing", "custom_app.config"));

            Assert.AreEqual("IgniteConfigurationSection with name 'igniteConfigurationMissing' in file " +
                            "'custom_app.config' is defined in <configSections>, but not present in configuration.",
                ex.Message);
        }
    }
}
