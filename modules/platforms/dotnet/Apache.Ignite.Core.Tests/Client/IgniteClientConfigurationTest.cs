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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Configuration;
    using System.IO;
    using System.Text;
    using System.Xml;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteClientConfiguration"/>.
    /// </summary>
    public class IgniteClientConfigurationTest
    {
        /// <summary>
        /// Tests the defaults.
        /// </summary>
        [Test]
        public void TestDefaults()
        {
            var cfg = new IgniteClientConfiguration();

            Assert.AreEqual(IgniteClientConfiguration.DefaultPort, cfg.Port);
            Assert.AreEqual(IgniteClientConfiguration.DefaultSocketBufferSize, cfg.SocketReceiveBufferSize);
            Assert.AreEqual(IgniteClientConfiguration.DefaultSocketBufferSize, cfg.SocketSendBufferSize);
            Assert.AreEqual(IgniteClientConfiguration.DefaultTcpNoDelay, cfg.TcpNoDelay);
            Assert.AreEqual(IgniteClientConfiguration.DefaultSocketTimeout, cfg.SocketTimeout);
        }

        /// <summary>
        /// Tests the FromXml method.
        /// </summary>
        [Test]
        public void TestFromXml()
        {
            // Empty (root element name does not matter).
            var cfg = IgniteClientConfiguration.FromXml("<foo />");
            Assert.AreEqual(new IgniteClientConfiguration().ToXml(), cfg.ToXml());

            // Properties.
            cfg = IgniteClientConfiguration.FromXml("<a host='h' port='123' />");
            Assert.AreEqual("h", cfg.Host);
            Assert.AreEqual(123, cfg.Port);

            // Full config.
            var fullCfg = new IgniteClientConfiguration
            {
                Host = "test1",
                Port = 345,
                SocketReceiveBufferSize = 222,
                SocketSendBufferSize = 333,
                TcpNoDelay = false,
                SocketTimeout = TimeSpan.FromSeconds(15),
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = false,
                    KeepDeserialized = false,
                    Types = new[] {"foo", "bar"}
                }
            };

            using (var xmlReader = XmlReader.Create(Path.Combine("Config", "Client", "IgniteClientConfiguration.xml")))
            {
                xmlReader.MoveToContent();

                cfg = IgniteClientConfiguration.FromXml(xmlReader);

                Assert.AreEqual(cfg.ToXml(), fullCfg.ToXml());
            }
        }

        /// <summary>
        /// Tests the ToXml method.
        /// </summary>
        [Test]
        public void TestToXml()
        {
            // Empty config.
            Assert.AreEqual("<?xml version=\"1.0\" encoding=\"utf-16\"?>" + Environment.NewLine +
                            "<igniteClientConfiguration " +
                            "xmlns=\"http://ignite.apache.org/schema/dotnet/IgniteClientConfigurationSection\" />",
                new IgniteClientConfiguration().ToXml());

            // Some properties.
            var cfg = new IgniteClientConfiguration
            {
                Host = "myHost",
                Port = 123
            };

            Assert.AreEqual("<?xml version=\"1.0\" encoding=\"utf-16\"?>" + Environment.NewLine +
                            "<igniteClientConfiguration host=\"myHost\" port=\"123\" " +
                            "xmlns=\"http://ignite.apache.org/schema/dotnet/IgniteClientConfigurationSection\" />",
                cfg.ToXml());

            // Nested objects.
            cfg = new IgniteClientConfiguration
            {
                SocketSendBufferSize = 2,
                BinaryConfiguration = new BinaryConfiguration {CompactFooter = false}
            };

            Assert.IsTrue(cfg.ToXml().Contains("<binaryConfiguration compactFooter=\"false\" />"), cfg.ToXml());

            // Custom element name.
            var sb = new StringBuilder();

            using (var xmlWriter = XmlWriter.Create(sb))
            {
                new IgniteClientConfiguration().ToXml(xmlWriter, "fooBar");
            }

            Assert.AreEqual("<?xml version=\"1.0\" encoding=\"utf-16\"?><fooBar " +
                            "xmlns=\"http://ignite.apache.org/schema/dotnet/IgniteClientConfigurationSection\" />",
                sb.ToString());
        }

        /// <summary>
        /// Tests client start from application configuration.
        /// </summary>
        [Test]
        public void TestStartFromAppConfig()
        {
            using (Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                // Custom file.
                using (var client = Ignition.StartClient("igniteClientConfiguration", "custom_app.config"))
                {
                    Assert.AreEqual(512, client.GetConfiguration().SocketSendBufferSize);
                }

                // Missing file.
                var ex = Assert.Throws<ConfigurationErrorsException>(() => Ignition.StartClient("foo", "bar"));
                Assert.AreEqual("Specified config file does not exist: bar", ex.Message);

#if !NETCOREAPP2_0  // Test runners do not pick up default config.
                // Default section.
                using (var client = Ignition.StartClient())
                {
                    Assert.AreEqual("127.0.0.1", client.GetConfiguration().Host);
                    Assert.AreEqual(0, client.GetConfiguration().SocketSendBufferSize);
                }

                // Custom section.
                using (var client = Ignition.StartClient("igniteClientConfiguration2"))
                {
                    Assert.AreEqual("127.0.0.1", client.GetConfiguration().Host);
                    Assert.AreEqual(2048, client.GetConfiguration().SocketSendBufferSize);
                }

                // Missing section content.
                ex = Assert.Throws<ConfigurationErrorsException>(() => 
                    Ignition.StartClient("igniteClientConfiguration3"));
                Assert.AreEqual("IgniteClientConfigurationSection with name 'igniteClientConfiguration3' is " +
                                "defined in <configSections>, but not present in configuration.", ex.Message);

                // Missing section.
                ex = Assert.Throws<ConfigurationErrorsException>(() => Ignition.StartClient("foo"));
                Assert.AreEqual("Could not find IgniteClientConfigurationSection with name 'foo'.", ex.Message);
#endif
            }
        }

#if !NETCOREAPP2_0
        /// <summary>
        /// Tests the schema validation.
        /// </summary>
        [Test]
        public void TestSchemaValidation()
        {
            var xml = File.ReadAllText("Config\\Client\\IgniteClientConfiguration.xml");
            var xmlns = "http://ignite.apache.org/schema/dotnet/IgniteClientConfigurationSection";
            var schemaFile = "IgniteClientConfigurationSection.xsd";

            IgniteConfigurationSerializerTest.CheckSchemaValidation(xml, xmlns, schemaFile);
        }

        /// <summary>
        /// Tests that all properties are present in the schema.
        /// </summary>
        [Test]
        public void TestAllPropertiesArePresentInSchema()
        {
            IgniteConfigurationSerializerTest.CheckAllPropertiesArePresentInSchema(
                "IgniteClientConfigurationSection.xsd", "igniteClientConfiguration", typeof(IgniteClientConfiguration));
        }
#endif
    }
}
