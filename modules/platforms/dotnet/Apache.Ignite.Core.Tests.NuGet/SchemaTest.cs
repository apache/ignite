/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
