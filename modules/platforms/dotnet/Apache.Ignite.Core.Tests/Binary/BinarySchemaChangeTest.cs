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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests that schema can be changed for an existing binary type.
    /// </summary>
    public class BinarySchemaChangeTest
    {
        /** */
        private const string CacheName = "TEST";

        /** */
        private IIgnite _grid;

        /** */
        private IIgnite _clientGrid;

        [SetUp]
        public void SetUp()
        {
            _grid = Ignition.Start(Config("Config\\Compute\\compute-grid1.xml"));
            _clientGrid = Ignition.Start(Config("Config\\Compute\\compute-grid3.xml"));
            _grid.GetOrCreateCache<int, object>(new CacheConfiguration
            {
                Name = CacheName,
                CacheMode = CacheMode.Replicated
            });
        }

        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public void TestChangedSchema()
        {
            var objWith2Fields = new TestObj { Fields = new[] { "Field1", "Field2" }, Field1 = "test1", Field2 = "test2" };
            var objWith1Field = new TestObj { Fields = new[] { "Field1" }, Field1 = "test1" };

            _clientGrid.GetOrCreateCache<int, TestObj>(CacheName).Put(1, objWith2Fields);
            _grid.GetOrCreateCache<int, TestObj>(CacheName).Get(1);
            _grid.GetOrCreateCache<int, TestObj>(CacheName).Remove(1);
            _clientGrid.GetCache<int, TestObj>(CacheName).Put(1, objWith1Field);
            _grid.GetCache<int, TestObj>(CacheName).Get(1);
        }

        private static IgniteConfiguration Config(string springUrl)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springUrl,
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = BinaryBasicNameMapper.SimpleNameInstance
                }
            };
        }

        private class TestObj : IBinarizable
        {
            public string[] Fields { get; set; }

            public string Field1 { get; set; }

            public string Field2 { get; set; }

            public string Field3 { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteStringArray(nameof(Fields), Fields);

                if (Fields.Contains("Field1"))
                    writer.WriteString(nameof(Field1), Field1);
                if (Fields.Contains("Field2"))
                    writer.WriteString(nameof(Field2), Field2);
                if (Fields.Contains("Field3"))
                    writer.WriteString(nameof(Field3), Field3);

            }

            public void ReadBinary(IBinaryReader reader)
            {
                Fields = reader.ReadStringArray(nameof(Fields));

                if (Fields.Contains("Field1"))
                    Field1 = reader.ReadString(nameof(Field1));
                if (Fields.Contains("Field2"))
                    Field2 = reader.ReadString(nameof(Field2));
                if (Fields.Contains("Field3"))
                    Field3 = reader.ReadString(nameof(Field3));
            }
        }
    }
}
