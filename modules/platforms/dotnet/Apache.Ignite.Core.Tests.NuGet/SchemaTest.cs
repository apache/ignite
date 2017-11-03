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

namespace Apache.Ignite.Core.Tests.NuGet
{
    using System.IO;
    using System.Xml;
    using System.Xml.Schema;
    using NUnit.Framework;

    /// <summary>
    /// Tests the Apache.Ignite.Schema package.
    /// </summary>
    public class SchemaTest
    {
        /// <summary>
        /// Tests that schema exists and validates XML config properly..
        /// </summary>
        [Test]
        public void TestSchemavalidation()
        {
            Assert.IsTrue(File.Exists("IgniteConfigurationSection.xsd"));

            // Valid schema
            CheckSchemaValidation(@"<igniteConfiguration xmlns='http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection' igniteInstanceName='myGrid'><binaryConfiguration /></igniteConfiguration>");

            // Invalid schema
            Assert.Throws<XmlSchemaValidationException>(() => CheckSchemaValidation(
                @"<igniteConfiguration xmlns='http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection' invalidAttr='myGrid' />"));
        }

        /// <summary>
        /// Checks the schema validation.
        /// </summary>
        /// <param name="xml">The XML.</param>
        private static void CheckSchemaValidation(string xml)
        {
            var document = new XmlDocument();

            document.Schemas.Add("http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection",
                XmlReader.Create("IgniteConfigurationSection.xsd"));

            document.Load(new StringReader(xml));

            document.Validate(null);
        }
    }
}
