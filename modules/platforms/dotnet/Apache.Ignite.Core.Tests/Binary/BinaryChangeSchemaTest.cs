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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    public class BinaryChangeSchemaTest
    {
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
            var objWith2Fields = new TestObj { fields = new List<string> { "Field1", "Field2" }, Field1 = "test1", Field2 = "test2" };
            var objWith1Field = new TestObj { fields = new List<string> { "Field1" }, Field1 = "test1" };

            _clientGrid.GetOrCreateCache<int, TestObj>(CacheName).Put(1, objWith2Fields);
            _grid.GetOrCreateCache<int, TestObj>(CacheName).TryGet(1, out var res);
            _grid.GetOrCreateCache<int, TestObj>(CacheName).Remove(1);
            _clientGrid.GetCache<int, TestObj>(CacheName).Put(1, objWith1Field);
            _grid.GetCache<int, TestObj>(CacheName).TryGet(1, out res);
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

        public class TestObj : IBinarizable
        {
            public List<string> fields = new List<string>();
            public string Field1;
            public string Field2;
            public string Field3;

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteCollection(nameof(fields), fields);

                if (fields.Contains("Field1"))
                    writer.WriteString(nameof(Field1), Field1);
                if (fields.Contains("Field2"))
                    writer.WriteString(nameof(Field2), Field2);
                if (fields.Contains("Field3"))
                    writer.WriteString(nameof(Field3), Field3);

            }

            /// <inheritdoc />
            public void ReadBinary(IBinaryReader reader)
            {
                fields = (List<string>)reader.ReadCollection(nameof(fields), (size) => new List<string>(size),
                    (col, obj) => ((List<string>)col).Add((string)obj));

                if (fields.Contains("Field1"))
                    Field1 = reader.ReadString(nameof(Field1));
                if (fields.Contains("Field2"))
                    Field2 = reader.ReadString(nameof(Field2));
                if (fields.Contains("Field3"))
                    Field3 = reader.ReadString(nameof(Field3));
            }
        }
    }
}
