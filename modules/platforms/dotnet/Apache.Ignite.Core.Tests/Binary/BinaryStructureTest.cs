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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Contains tests for binary type structure.
    /// </summary>
    [TestFixture]
    public class BinaryStructureTest
    {
        /** Repeat count. */
        public static readonly int RepeatCnt = 10;

        /** Objects per mode. */
        public static readonly int ObjectsPerMode = 5;

        /// <summary>
        /// Test object write with different structures.
        /// </summary>
        [Test]
        public void TestStructure()
        {
            for (int i = 1; i <= RepeatCnt; i++)
            {
                Console.WriteLine(">>> Iteration started: " + i);

                // 1. Generate and shuffle objects.
                IList<BranchedType> objs = new List<BranchedType>();

                for (int j = 0; j < 6 * ObjectsPerMode; j++)
                    objs.Add(new BranchedType((j%6) + 1));

                objs = IgniteUtils.Shuffle(objs);

                // 2. Create new marshaller.
                BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(typeof(BranchedType));

                BinaryConfiguration cfg = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration> { typeCfg }
                };

                Marshaller marsh = new Marshaller(cfg);

                // 3. Marshal all data and ensure deserialized object is fine.
                // Use single stream to test object offsets
                using (var stream = new BinaryHeapStream(128))
                {
                    var writer = marsh.StartMarshal(stream);

                    foreach (var obj in objs)
                    {
                        Console.WriteLine(">>> Write object [mode=" + obj.mode + ']');

                        writer.WriteObject(obj);

                    }

                    stream.Seek(0, SeekOrigin.Begin);

                    var reader = marsh.StartUnmarshal(stream);

                    foreach (var obj in objs)
                    {
                        var other = reader.ReadObject<BranchedType>();

                        Assert.IsTrue(obj.Equals(other));
                    }
                }
                
                Console.WriteLine();

                // 4. Ensure that all fields are recorded.
                var desc = marsh.GetDescriptor(typeof (BranchedType));

                CollectionAssert.AreEquivalent(new[] {"mode", "f2", "f3", "f4", "f5", "f6", "f7", "f8"},
                    desc.WriterTypeStructure.FieldTypes.Keys);
            }
        }

        /// <summary>
        /// Tests that nested raw object does not inherit outer schema.
        /// </summary>
        [Test]
        public void TestNestedRaw()
        {
            var marsh = new Marshaller(new BinaryConfiguration(typeof(RawContainer), typeof(RawNested)));

            var obj = new RawContainer {Int = 3, Raw = new RawNested {Int = 5}};

            var res = marsh.Unmarshal<RawContainer>(marsh.Marshal(obj));

            Assert.AreEqual(obj.Int, res.Int);
            Assert.AreEqual(0, res.Raw.Int);  // Int is not written and can't be read.
        }

        /// <summary>
        /// Tests that nested object schemas do not interfere.
        /// </summary>
        [Test]
        public void TestNested()
        {
            var marsh = new Marshaller(new BinaryConfiguration(typeof(Container), typeof(Nested)));

            var obj = new Container
            {
                Foo = 2,
                Bar = 4,
                Nested = new Nested
                {
                    Baz = 3,
                    Qux = 5
                }
            };

            var res = marsh.Unmarshal<Container>(marsh.Marshal(obj));

            Assert.AreEqual(2, res.Foo);
            Assert.AreEqual(4, res.Bar);
            Assert.AreEqual(3, res.Nested.Baz);
            Assert.AreEqual(5, res.Nested.Qux);
        }
    }

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class BranchedType : IBinarizable
    {
        public int mode;
        public int f2;
        public int f3;
        public int f4;
        public int f5;
        public int f6;
        public int f7;
        public int f8;

        public BranchedType(int mode)
        {
            this.mode = mode;

            switch (mode)
            {
                case 1:
                    f2 = 2;

                    break;

                case 2:
                    f2 = 2;
                    f3 = 3;
                    f4 = 4;

                    break;

                case 3:
                    f2 = 2;
                    f3 = 3;
                    f5 = 5;

                    break;

                case 4:
                    f2 = 2;
                    f3 = 3;
                    f5 = 5;
                    f6 = 6;

                    break;

                case 5:
                    f2 = 2;
                    f3 = 3;
                    f7 = 7;

                    break;

                case 6:
                    f8 = 8;

                    break;
            }
        }

        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("mode", mode);

            switch (mode)
            {
                case 1:
                    writer.WriteInt("f2", f2);

                    break;

                case 2:
                    writer.WriteInt("f2", f2);
                    writer.WriteInt("f3", f3);
                    writer.WriteInt("f4", f4);

                    break;

                case 3:
                    writer.WriteInt("f2", f2);
                    writer.WriteInt("f3", f3);
                    writer.WriteInt("f5", f5);

                    break;

                case 4:
                    writer.WriteInt("f2", f2);
                    writer.WriteInt("f3", f3);
                    writer.WriteInt("f5", f5);
                    writer.WriteInt("f6", f6);

                    break;

                case 5:
                    writer.WriteInt("f2", f2);
                    writer.WriteInt("f3", f3);
                    writer.WriteInt("f7", f7);

                    break;

                case 6:
                    writer.WriteInt("f8", f8);

                    break;
            }
        }

        public void ReadBinary(IBinaryReader reader)
        {
            mode = reader.ReadInt("mode");

            switch (mode)
            {
                case 1:
                    f2 = reader.ReadInt("f2");

                    break;

                case 2:
                    f4 = reader.ReadInt("f4");
                    f3 = reader.ReadInt("f3");
                    f2 = reader.ReadInt("f2");

                    break;

                case 3:
                    f5 = reader.ReadInt("f5");
                    f2 = reader.ReadInt("f2");
                    f3 = reader.ReadInt("f3");

                    break;

                case 4:
                    f5 = reader.ReadInt("f5");
                    f6 = reader.ReadInt("f6");
                    f2 = reader.ReadInt("f2");
                    f3 = reader.ReadInt("f3");

                    break;

                case 5:
                    f3 = reader.ReadInt("f3");
                    f2 = reader.ReadInt("f2");
                    f7 = reader.ReadInt("f7");

                    break;

                case 6:
                    f8 = reader.ReadInt("f8");

                    break;
            }
        }

        public bool Equals(BranchedType other)
        {
            return mode == other.mode && f2 == other.f2 && f3 == other.f3 && f4 == other.f4 && f5 == other.f5 &&
                   f6 == other.f6 && f7 == other.f7 && f8 == other.f8;
        }
    }

    public class RawContainer : IBinarizable
    {
        public int Int { get; set; }

        public RawNested Raw { get; set; }

        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("int", Int);
            writer.WriteObject("raw", Raw);
        }

        public void ReadBinary(IBinaryReader reader)
        {
            Int = reader.ReadInt("int");
            Raw = reader.ReadObject<RawNested>("raw");
        }
    }

    public class RawNested : IBinarizable
    {
        public int Int { get; set; }

        public void WriteBinary(IBinaryWriter writer)
        {
            // Write only raw data.
            writer.GetRawWriter().WriteIntArray(Enumerable.Range(1, 100).ToArray());
        }

        public void ReadBinary(IBinaryReader reader)
        {
            // Attempt to read even though we did not write fields.
            // If schema is carried over, there will be a broken result.
            Int = reader.ReadInt("int");
        }
    }

    public class Container : IBinarizable
    {
        public int Foo { get; set; }
        public int Bar { get; set; }
        public Nested Nested { get; set; }

        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("foo", Foo);
            writer.WriteInt("bar", Bar);
            writer.WriteObject("nested", Nested);
        }

        public void ReadBinary(IBinaryReader reader)
        {
            // Read in reverse order to defeat structure optimization.
            Bar = reader.ReadInt("bar");
            Foo = reader.ReadInt("foo");
            Nested = reader.ReadObject<Nested>("nested");
        }
    }

    public class Nested : IBinarizable
    {
        public int Baz { get; set; }
        public int Qux { get; set; }

        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("baz", Baz);
            writer.WriteInt("qux", Qux);
        }

        public void ReadBinary(IBinaryReader reader)
        {
            // Read in reverse order to defeat structure optimization.
            Qux = reader.ReadInt("qux");
            Baz = reader.ReadInt("baz");
        }
    }
}
