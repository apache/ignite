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

// ReSharper disable UnassignedField.Global
// ReSharper disable CollectionNeverUpdated.Global
namespace Apache.Ignite.Core.Tests.Portable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// Portable builder self test.
    /// </summary>
    public class PortableApiSelfTest
    {
        /** Undefined type: Empty. */
        private const string TypeEmpty = "EmptyUndefined";

        /** Grid. */
        private Ignite _grid;

        /** Marshaller. */
        private PortableMarshaller _marsh;

        /// <summary>
        /// Set up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();

            var cfg = new IgniteConfiguration
            {
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations = new List<PortableTypeConfiguration>
                    {
                        new PortableTypeConfiguration(typeof (Empty)),
                        new PortableTypeConfiguration(typeof (Primitives)),
                        new PortableTypeConfiguration(typeof (PrimitiveArrays)),
                        new PortableTypeConfiguration(typeof (StringDateGuidEnum)),
                        new PortableTypeConfiguration(typeof (WithRaw)),
                        new PortableTypeConfiguration(typeof (MetaOverwrite)),
                        new PortableTypeConfiguration(typeof (NestedOuter)),
                        new PortableTypeConfiguration(typeof (NestedInner)),
                        new PortableTypeConfiguration(typeof (MigrationOuter)),
                        new PortableTypeConfiguration(typeof (MigrationInner)),
                        new PortableTypeConfiguration(typeof (InversionOuter)),
                        new PortableTypeConfiguration(typeof (InversionInner)),
                        new PortableTypeConfiguration(typeof (CompositeOuter)),
                        new PortableTypeConfiguration(typeof (CompositeInner)),
                        new PortableTypeConfiguration(typeof (CompositeArray)),
                        new PortableTypeConfiguration(typeof (CompositeContainer)),
                        new PortableTypeConfiguration(typeof (ToPortable)),
                        new PortableTypeConfiguration(typeof (Remove)),
                        new PortableTypeConfiguration(typeof (RemoveInner)),
                        new PortableTypeConfiguration(typeof (BuilderInBuilderOuter)),
                        new PortableTypeConfiguration(typeof (BuilderInBuilderInner)),
                        new PortableTypeConfiguration(typeof (BuilderCollection)),
                        new PortableTypeConfiguration(typeof (BuilderCollectionItem)),
                        new PortableTypeConfiguration(typeof (DecimalHolder)),
                        new PortableTypeConfiguration(TypeEmpty),
                        TypeConfigurationNoMeta(typeof (EmptyNoMeta)),
                        TypeConfigurationNoMeta(typeof (ToPortableNoMeta))
                    },
                    DefaultIdMapper = new IdMapper()
                },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = new List<string>
                {
                    "-ea",
                    "-Xcheck:jni",
                    "-Xms4g",
                    "-Xmx4g",
                    "-DIGNITE_QUIET=false",
                    "-Xnoagent",
                    "-Djava.compiler=NONE",
                    "-Xdebug",
                    "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005",
                    "-XX:+HeapDumpOnOutOfMemoryError"
                },
                SpringConfigUrl = "config\\portable.xml"
            };

            _grid = (Ignite) Ignition.Start(cfg);

            _marsh = _grid.Marshaller;
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void TearDown()
        {
            if (_grid != null)
                Ignition.Stop(_grid.Name, true);

            _grid = null;
        }

        /// <summary>
        /// Ensure that portable engine is able to work with type names, which are not configured.
        /// </summary>
        [Test]
        public void TestNonConfigured()
        {
            string typeName1 = "Type1";
            string typeName2 = "Type2";
            string field1 = "field1";
            string field2 = "field2";

            // 1. Ensure that builder works fine.
            IPortableObject portObj1 = _grid.GetPortables().GetBuilder(typeName1).SetField(field1, 1).Build();

            Assert.AreEqual(typeName1, portObj1.GetMetadata().TypeName);
            Assert.AreEqual(1, portObj1.GetMetadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.GetMetadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameInt, portObj1.GetMetadata().GetFieldTypeName(field1));

            Assert.AreEqual(1, portObj1.GetField<int>(field1));

            // 2. Ensure that object can be unmarshalled without deserialization.
            byte[] data = ((PortableUserObject) portObj1).Data;

            portObj1 = _grid.Marshaller.Unmarshal<IPortableObject>(data, PortableMode.ForcePortable);

            Assert.AreEqual(typeName1, portObj1.GetMetadata().TypeName);
            Assert.AreEqual(1, portObj1.GetMetadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.GetMetadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameInt, portObj1.GetMetadata().GetFieldTypeName(field1));

            Assert.AreEqual(1, portObj1.GetField<int>(field1));

            // 3. Ensure that we can nest one anonymous object inside another
            IPortableObject portObj2 =
                _grid.GetPortables().GetBuilder(typeName2).SetField(field2, portObj1).Build();

            Assert.AreEqual(typeName2, portObj2.GetMetadata().TypeName);
            Assert.AreEqual(1, portObj2.GetMetadata().Fields.Count);
            Assert.AreEqual(field2, portObj2.GetMetadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameObject, portObj2.GetMetadata().GetFieldTypeName(field2));

            portObj1 = portObj2.GetField<IPortableObject>(field2);

            Assert.AreEqual(typeName1, portObj1.GetMetadata().TypeName);
            Assert.AreEqual(1, portObj1.GetMetadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.GetMetadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameInt, portObj1.GetMetadata().GetFieldTypeName(field1));

            Assert.AreEqual(1, portObj1.GetField<int>(field1));

            // 4. Ensure that we can unmarshal object with other nested object.
            data = ((PortableUserObject) portObj2).Data;

            portObj2 = _grid.Marshaller.Unmarshal<IPortableObject>(data, PortableMode.ForcePortable);

            Assert.AreEqual(typeName2, portObj2.GetMetadata().TypeName);
            Assert.AreEqual(1, portObj2.GetMetadata().Fields.Count);
            Assert.AreEqual(field2, portObj2.GetMetadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameObject, portObj2.GetMetadata().GetFieldTypeName(field2));

            portObj1 = portObj2.GetField<IPortableObject>(field2);

            Assert.AreEqual(typeName1, portObj1.GetMetadata().TypeName);
            Assert.AreEqual(1, portObj1.GetMetadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.GetMetadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameInt, portObj1.GetMetadata().GetFieldTypeName(field1));

            Assert.AreEqual(1, portObj1.GetField<int>(field1));
        }

        /// <summary>
        /// Test "ToPortable()" method.
        /// </summary>
        [Test]
        public void TestToPortable()
        {
            DateTime date = DateTime.Now.ToUniversalTime();
            Guid guid = Guid.NewGuid();

            IPortables api = _grid.GetPortables();

            // 1. Primitives.
            Assert.AreEqual(1, api.ToPortable<byte>((byte)1));
            Assert.AreEqual(1, api.ToPortable<short>((short)1));
            Assert.AreEqual(1, api.ToPortable<int>(1));
            Assert.AreEqual(1, api.ToPortable<long>((long)1));

            Assert.AreEqual((float)1, api.ToPortable<float>((float)1));
            Assert.AreEqual((double)1, api.ToPortable<double>((double)1));

            Assert.AreEqual(true, api.ToPortable<bool>(true));
            Assert.AreEqual('a', api.ToPortable<char>('a'));

            // 2. Special types.
            Assert.AreEqual("a", api.ToPortable<string>("a"));
            Assert.AreEqual(date, api.ToPortable<DateTime>(date));
            Assert.AreEqual(guid, api.ToPortable<Guid>(guid));
            Assert.AreEqual(TestEnum.One, api.ToPortable<TestEnum>(TestEnum.One));

            // 3. Arrays.
            Assert.AreEqual(new byte[] { 1 }, api.ToPortable<byte[]>(new byte[] { 1 }));
            Assert.AreEqual(new short[] { 1 }, api.ToPortable<short[]>(new short[] { 1 }));
            Assert.AreEqual(new[] { 1 }, api.ToPortable<int[]>(new[] { 1 }));
            Assert.AreEqual(new long[] { 1 }, api.ToPortable<long[]>(new long[] { 1 }));

            Assert.AreEqual(new float[] { 1 }, api.ToPortable<float[]>(new float[] { 1 }));
            Assert.AreEqual(new double[] { 1 }, api.ToPortable<double[]>(new double[] { 1 }));

            Assert.AreEqual(new[] { true }, api.ToPortable<bool[]>(new[] { true }));
            Assert.AreEqual(new[] { 'a' }, api.ToPortable<char[]>(new[] { 'a' }));

            Assert.AreEqual(new[] { "a" }, api.ToPortable<string[]>(new[] { "a" }));
            Assert.AreEqual(new[] { date }, api.ToPortable<DateTime[]>(new[] { date }));
            Assert.AreEqual(new[] { guid }, api.ToPortable<Guid[]>(new[] { guid }));
            Assert.AreEqual(new[] { TestEnum.One }, api.ToPortable<TestEnum[]>(new[] { TestEnum.One }));

            // 4. Objects.
            IPortableObject portObj = api.ToPortable<IPortableObject>(new ToPortable(1));

            Assert.AreEqual(typeof(ToPortable).Name, portObj.GetMetadata().TypeName);
            Assert.AreEqual(1, portObj.GetMetadata().Fields.Count);
            Assert.AreEqual("Val", portObj.GetMetadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameInt, portObj.GetMetadata().GetFieldTypeName("Val"));

            Assert.AreEqual(1, portObj.GetField<int>("val"));
            Assert.AreEqual(1, portObj.Deserialize<ToPortable>().Val);

            portObj = api.ToPortable<IPortableObject>(new ToPortableNoMeta(1));

            Assert.AreEqual(1, portObj.GetMetadata().Fields.Count);

            Assert.AreEqual(1, portObj.GetField<int>("Val"));
            Assert.AreEqual(1, portObj.Deserialize<ToPortableNoMeta>().Val);

            // 5. Object array.
            var portObjArr = api.ToPortable<object[]>(new object[] {new ToPortable(1)})
                .OfType<IPortableObject>().ToArray();

            Assert.AreEqual(1, portObjArr.Length);
            Assert.AreEqual(1, portObjArr[0].GetField<int>("Val"));
            Assert.AreEqual(1, portObjArr[0].Deserialize<ToPortable>().Val);
        }

        /// <summary>
        /// Test builder field remove logic.
        /// </summary>
        [Test]
        public void TestRemove()
        {
            // Create empty object.
            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(Remove)).Build();

            Assert.IsNull(portObj.GetField<object>("val"));
            Assert.IsNull(portObj.Deserialize<Remove>().Val);

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(Remove).Name, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);

            // Populate it with field.
            IPortableBuilder builder = _grid.GetPortables().GetBuilder(portObj);

            Assert.IsNull(builder.GetField<object>("val"));

            object val = 1;

            builder.SetField("val", val);

            Assert.AreEqual(val, builder.GetField<object>("val"));

            portObj = builder.Build();

            Assert.AreEqual(val, portObj.GetField<object>("val"));
            Assert.AreEqual(val, portObj.Deserialize<Remove>().Val);

            meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(Remove).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("val", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameObject, meta.GetFieldTypeName("val"));

            // Perform field remove.
            builder = _grid.GetPortables().GetBuilder(portObj);

            Assert.AreEqual(val, builder.GetField<object>("val"));

            builder.RemoveField("val");
            Assert.IsNull(builder.GetField<object>("val"));

            builder.SetField("val", val);
            Assert.AreEqual(val, builder.GetField<object>("val"));

            builder.RemoveField("val");
            Assert.IsNull(builder.GetField<object>("val"));

            portObj = builder.Build();

            Assert.IsNull(portObj.GetField<object>("val"));
            Assert.IsNull(portObj.Deserialize<Remove>().Val);

            // Test correct removal of field being referenced by handle somewhere else.
            RemoveInner inner = new RemoveInner(2);

            portObj = _grid.GetPortables().GetBuilder(typeof(Remove))
                .SetField("val", inner)
                .SetField("val2", inner)
                .Build();

            portObj = _grid.GetPortables().GetBuilder(portObj).RemoveField("val").Build();

            Remove obj = portObj.Deserialize<Remove>();

            Assert.IsNull(obj.Val);
            Assert.AreEqual(2, obj.Val2.Val);
        }

        /// <summary>
        /// Test builder-in-builder scenario.
        /// </summary>
        [Test]
        public void TestBuilderInBuilder()
        {
            // Test different builders assembly.
            IPortableBuilder builderOuter = _grid.GetPortables().GetBuilder(typeof(BuilderInBuilderOuter));
            IPortableBuilder builderInner = _grid.GetPortables().GetBuilder(typeof(BuilderInBuilderInner));

            builderOuter.SetField<object>("inner", builderInner);
            builderInner.SetField<object>("outer", builderOuter);

            IPortableObject outerPortObj = builderOuter.Build();

            IPortableMetadata meta = outerPortObj.GetMetadata();

            Assert.AreEqual(typeof(BuilderInBuilderOuter).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("inner", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameObject, meta.GetFieldTypeName("inner"));

            IPortableObject innerPortObj = outerPortObj.GetField<IPortableObject>("inner");

            meta = innerPortObj.GetMetadata();

            Assert.AreEqual(typeof(BuilderInBuilderInner).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("outer", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameObject, meta.GetFieldTypeName("outer"));

            BuilderInBuilderOuter outer = outerPortObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer, outer.Inner.Outer);

            // Test same builders assembly.
            innerPortObj = _grid.GetPortables().GetBuilder(typeof(BuilderInBuilderInner)).Build();

            outerPortObj = _grid.GetPortables().GetBuilder(typeof(BuilderInBuilderOuter))
                .SetField("inner", innerPortObj)
                .SetField("inner2", innerPortObj)
                .Build();

            meta = outerPortObj.GetMetadata();

            Assert.AreEqual(typeof(BuilderInBuilderOuter).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.IsTrue(meta.Fields.Contains("inner"));
            Assert.IsTrue(meta.Fields.Contains("inner2"));
            Assert.AreEqual(PortableTypeNames.TypeNameObject, meta.GetFieldTypeName("inner"));
            Assert.AreEqual(PortableTypeNames.TypeNameObject, meta.GetFieldTypeName("inner2"));

            outer = outerPortObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer.Inner, outer.Inner2);

            builderOuter = _grid.GetPortables().GetBuilder(outerPortObj);
            IPortableBuilder builderInner2 = builderOuter.GetField<IPortableBuilder>("inner2");

            builderInner2.SetField("outer", builderOuter);

            outerPortObj = builderOuter.Build();

            outer = outerPortObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer, outer.Inner.Outer);
            Assert.AreSame(outer.Inner, outer.Inner2);
        }

        /// <summary>
        /// Test for decimals building.
        /// </summary>
        [Test]
        public void TestDecimals()
        {
            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(DecimalHolder))
                .SetField("val", decimal.One)
                .SetField("valArr", new decimal?[] { decimal.MinusOne })
                .Build();

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(DecimalHolder).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.IsTrue(meta.Fields.Contains("val"));
            Assert.IsTrue(meta.Fields.Contains("valArr"));
            Assert.AreEqual(PortableTypeNames.TypeNameDecimal, meta.GetFieldTypeName("val"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayDecimal, meta.GetFieldTypeName("valArr"));

            Assert.AreEqual(decimal.One, portObj.GetField<decimal>("val"));
            Assert.AreEqual(new decimal?[] { decimal.MinusOne }, portObj.GetField<decimal?[]>("valArr"));

            DecimalHolder obj = portObj.Deserialize<DecimalHolder>();

            Assert.AreEqual(decimal.One, obj.Val);
            Assert.AreEqual(new decimal?[] { decimal.MinusOne }, obj.ValArr);
        }

        /// <summary>
        /// Test for an object returning collection of builders.
        /// </summary>
        [Test]
        public void TestBuilderCollection()
        {
            // Test collection with single element.
            IPortableBuilder builderCol = _grid.GetPortables().GetBuilder(typeof(BuilderCollection));
            IPortableBuilder builderItem =
                _grid.GetPortables().GetBuilder(typeof(BuilderCollectionItem)).SetField("val", 1);

            builderCol.SetCollectionField("col", new ArrayList { builderItem });

            IPortableObject portCol = builderCol.Build();

            IPortableMetadata meta = portCol.GetMetadata();

            Assert.AreEqual(typeof(BuilderCollection).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("col", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameCollection, meta.GetFieldTypeName("col"));

            var portColItems = portCol.GetField<ArrayList>("col");

            Assert.AreEqual(1, portColItems.Count);

            var portItem = (IPortableObject) portColItems[0];

            meta = portItem.GetMetadata();

            Assert.AreEqual(typeof(BuilderCollectionItem).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("val", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TypeNameInt, meta.GetFieldTypeName("val"));

            BuilderCollection col = portCol.Deserialize<BuilderCollection>();

            Assert.IsNotNull(col.Col);
            Assert.AreEqual(1, col.Col.Count);
            Assert.AreEqual(1, ((BuilderCollectionItem) col.Col[0]).Val);

            // Add more portable objects to collection.
            builderCol = _grid.GetPortables().GetBuilder(portCol);

            IList builderColItems = builderCol.GetField<IList>("col");

            Assert.AreEqual(1, builderColItems.Count);

            PortableBuilderImpl builderColItem = (PortableBuilderImpl) builderColItems[0];

            builderColItem.SetField("val", 2); // Change nested value.

            builderColItems.Add(builderColItem); // Add the same object to check handles.
            builderColItems.Add(builderItem); // Add item from another builder.
            builderColItems.Add(portItem); // Add item in portable form.

            portCol = builderCol.Build();

            col = portCol.Deserialize<BuilderCollection>();

            Assert.AreEqual(4, col.Col.Count);

            var item0 = (BuilderCollectionItem) col.Col[0];
            var item1 = (BuilderCollectionItem) col.Col[1];
            var item2 = (BuilderCollectionItem) col.Col[2];
            var item3 = (BuilderCollectionItem) col.Col[3];

            Assert.AreEqual(2, item0.Val);

            Assert.AreSame(item0, item1);
            Assert.AreNotSame(item0, item2);
            Assert.AreNotSame(item0, item3);

            Assert.AreEqual(1, item2.Val);
            Assert.AreEqual(1, item3.Val);

            Assert.AreNotSame(item2, item3);

            // Test handle update inside collection.
            builderCol = _grid.GetPortables().GetBuilder(portCol);

            builderColItems = builderCol.GetField<IList>("col");

            ((PortableBuilderImpl) builderColItems[1]).SetField("val", 3);

            portCol = builderCol.Build();

            col = portCol.Deserialize<BuilderCollection>();

            item0 = (BuilderCollectionItem) col.Col[0];
            item1 = (BuilderCollectionItem) col.Col[1];

            Assert.AreEqual(3, item0.Val);
            Assert.AreSame(item0, item1);
        }

        /// <summary>
        /// Test build of an empty object.
        /// </summary>
        [Test]
        public void TestEmptyDefined()
        {
            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(Empty)).Build();

            Assert.IsNotNull(portObj);
            Assert.AreEqual(0, portObj.GetHashCode());

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.IsNotNull(meta);
            Assert.AreEqual(typeof(Empty).Name, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);

            Empty obj = portObj.Deserialize<Empty>();

            Assert.IsNotNull(obj);
        }

        /// <summary>
        /// Test build of an empty object with disabled metadata.
        /// </summary>
        [Test]
        public void TestEmptyNoMeta()
        {
            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(EmptyNoMeta)).Build();

            Assert.IsNotNull(portObj);
            Assert.AreEqual(0, portObj.GetHashCode());

            EmptyNoMeta obj = portObj.Deserialize<EmptyNoMeta>();

            Assert.IsNotNull(obj);
        }

        /// <summary>
        /// Test build of an empty undefined object.
        /// </summary>
        [Test]
        public void TestEmptyUndefined()
        {
            IPortableObject portObj = _grid.GetPortables().GetBuilder(TypeEmpty).Build();

            Assert.IsNotNull(portObj);
            Assert.AreEqual(0, portObj.GetHashCode());

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.IsNotNull(meta);
            Assert.AreEqual(TypeEmpty, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);
        }

        /// <summary>
        /// Test object rebuild with no changes.
        /// </summary>
        [Test]
        public void TestEmptyRebuild()
        {
            var portObj = (PortableUserObject) _grid.GetPortables().GetBuilder(typeof(EmptyNoMeta)).Build();

            PortableUserObject newPortObj = (PortableUserObject) _grid.GetPortables().GetBuilder(portObj).Build();

            Assert.AreEqual(portObj.Data, newPortObj.Data);
        }

        /// <summary>
        /// Test hash code alteration.
        /// </summary>
        [Test]
        public void TestHashCodeChange()
        {
            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(EmptyNoMeta)).SetHashCode(100).Build();

            Assert.AreEqual(100, portObj.GetHashCode());
        }

        /// <summary>
        /// Test primitive fields setting.
        /// </summary>
        [Test]
        public void TestPrimitiveFields()
        {
            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(Primitives))
                .SetField<byte>("fByte", 1)
                .SetField("fBool", true)
                .SetField<short>("fShort", 2)
                .SetField("fChar", 'a')
                .SetField("fInt", 3)
                .SetField<long>("fLong", 4)
                .SetField<float>("fFloat", 5)
                .SetField<double>("fDouble", 6)
                .SetHashCode(100)
                .Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(Primitives).Name, meta.TypeName);

            Assert.AreEqual(8, meta.Fields.Count);

            Assert.AreEqual(PortableTypeNames.TypeNameByte, meta.GetFieldTypeName("fByte"));
            Assert.AreEqual(PortableTypeNames.TypeNameBool, meta.GetFieldTypeName("fBool"));
            Assert.AreEqual(PortableTypeNames.TypeNameShort, meta.GetFieldTypeName("fShort"));
            Assert.AreEqual(PortableTypeNames.TypeNameChar, meta.GetFieldTypeName("fChar"));
            Assert.AreEqual(PortableTypeNames.TypeNameInt, meta.GetFieldTypeName("fInt"));
            Assert.AreEqual(PortableTypeNames.TypeNameLong, meta.GetFieldTypeName("fLong"));
            Assert.AreEqual(PortableTypeNames.TypeNameFloat, meta.GetFieldTypeName("fFloat"));
            Assert.AreEqual(PortableTypeNames.TypeNameDouble, meta.GetFieldTypeName("fDouble"));

            Assert.AreEqual(1, portObj.GetField<byte>("fByte"));
            Assert.AreEqual(true, portObj.GetField<bool>("fBool"));
            Assert.AreEqual(2, portObj.GetField<short>("fShort"));
            Assert.AreEqual('a', portObj.GetField<char>("fChar"));
            Assert.AreEqual(3, portObj.GetField<int>("fInt"));
            Assert.AreEqual(4, portObj.GetField<long>("fLong"));
            Assert.AreEqual(5, portObj.GetField<float>("fFloat"));
            Assert.AreEqual(6, portObj.GetField<double>("fDouble"));

            Primitives obj = portObj.Deserialize<Primitives>();

            Assert.AreEqual(1, obj.FByte);
            Assert.AreEqual(true, obj.FBool);
            Assert.AreEqual(2, obj.FShort);
            Assert.AreEqual('a', obj.FChar);
            Assert.AreEqual(3, obj.FInt);
            Assert.AreEqual(4, obj.FLong);
            Assert.AreEqual(5, obj.FFloat);
            Assert.AreEqual(6, obj.FDouble);

            // Overwrite.
            portObj = _grid.GetPortables().GetBuilder(portObj)
                .SetField<byte>("fByte", 7)
                .SetField("fBool", false)
                .SetField<short>("fShort", 8)
                .SetField("fChar", 'b')
                .SetField("fInt", 9)
                .SetField<long>("fLong", 10)
                .SetField<float>("fFloat", 11)
                .SetField<double>("fDouble", 12)
                .SetHashCode(200)
                .Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            Assert.AreEqual(7, portObj.GetField<byte>("fByte"));
            Assert.AreEqual(false, portObj.GetField<bool>("fBool"));
            Assert.AreEqual(8, portObj.GetField<short>("fShort"));
            Assert.AreEqual('b', portObj.GetField<char>("fChar"));
            Assert.AreEqual(9, portObj.GetField<int>("fInt"));
            Assert.AreEqual(10, portObj.GetField<long>("fLong"));
            Assert.AreEqual(11, portObj.GetField<float>("fFloat"));
            Assert.AreEqual(12, portObj.GetField<double>("fDouble"));

            obj = portObj.Deserialize<Primitives>();

            Assert.AreEqual(7, obj.FByte);
            Assert.AreEqual(false, obj.FBool);
            Assert.AreEqual(8, obj.FShort);
            Assert.AreEqual('b', obj.FChar);
            Assert.AreEqual(9, obj.FInt);
            Assert.AreEqual(10, obj.FLong);
            Assert.AreEqual(11, obj.FFloat);
            Assert.AreEqual(12, obj.FDouble);
        }

        /// <summary>
        /// Test primitive array fields setting.
        /// </summary>
        [Test]
        public void TestPrimitiveArrayFields()
        {
            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(PrimitiveArrays))
                .SetField("fByte", new byte[] { 1 })
                .SetField("fBool", new[] { true })
                .SetField("fShort", new short[] { 2 })
                .SetField("fChar", new[] { 'a' })
                .SetField("fInt", new[] { 3 })
                .SetField("fLong", new long[] { 4 })
                .SetField("fFloat", new float[] { 5 })
                .SetField("fDouble", new double[] { 6 })
                .SetHashCode(100)
                .Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(PrimitiveArrays).Name, meta.TypeName);

            Assert.AreEqual(8, meta.Fields.Count);

            Assert.AreEqual(PortableTypeNames.TypeNameArrayByte, meta.GetFieldTypeName("fByte"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayBool, meta.GetFieldTypeName("fBool"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayShort, meta.GetFieldTypeName("fShort"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayChar, meta.GetFieldTypeName("fChar"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayInt, meta.GetFieldTypeName("fInt"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayLong, meta.GetFieldTypeName("fLong"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayFloat, meta.GetFieldTypeName("fFloat"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayDouble, meta.GetFieldTypeName("fDouble"));

            Assert.AreEqual(new byte[] { 1 }, portObj.GetField<byte[]>("fByte"));
            Assert.AreEqual(new[] { true }, portObj.GetField<bool[]>("fBool"));
            Assert.AreEqual(new short[] { 2 }, portObj.GetField<short[]>("fShort"));
            Assert.AreEqual(new[] { 'a' }, portObj.GetField<char[]>("fChar"));
            Assert.AreEqual(new[] { 3 }, portObj.GetField<int[]>("fInt"));
            Assert.AreEqual(new long[] { 4 }, portObj.GetField<long[]>("fLong"));
            Assert.AreEqual(new float[] { 5 }, portObj.GetField<float[]>("fFloat"));
            Assert.AreEqual(new double[] { 6 }, portObj.GetField<double[]>("fDouble"));

            PrimitiveArrays obj = portObj.Deserialize<PrimitiveArrays>();

            Assert.AreEqual(new byte[] { 1 }, obj.FByte);
            Assert.AreEqual(new[] { true }, obj.FBool);
            Assert.AreEqual(new short[] { 2 }, obj.FShort);
            Assert.AreEqual(new[] { 'a' }, obj.FChar);
            Assert.AreEqual(new[] { 3 }, obj.FInt);
            Assert.AreEqual(new long[] { 4 }, obj.FLong);
            Assert.AreEqual(new float[] { 5 }, obj.FFloat);
            Assert.AreEqual(new double[] { 6 }, obj.FDouble);

            // Overwrite.
            portObj = _grid.GetPortables().GetBuilder(portObj)
                .SetField("fByte", new byte[] { 7 })
                .SetField("fBool", new[] { false })
                .SetField("fShort", new short[] { 8 })
                .SetField("fChar", new[] { 'b' })
                .SetField("fInt", new[] { 9 })
                .SetField("fLong", new long[] { 10 })
                .SetField("fFloat", new float[] { 11 })
                .SetField("fDouble", new double[] { 12 })
                .SetHashCode(200)
                .Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            Assert.AreEqual(new byte[] { 7 }, portObj.GetField<byte[]>("fByte"));
            Assert.AreEqual(new[] { false }, portObj.GetField<bool[]>("fBool"));
            Assert.AreEqual(new short[] { 8 }, portObj.GetField<short[]>("fShort"));
            Assert.AreEqual(new[] { 'b' }, portObj.GetField<char[]>("fChar"));
            Assert.AreEqual(new[] { 9 }, portObj.GetField<int[]>("fInt"));
            Assert.AreEqual(new long[] { 10 }, portObj.GetField<long[]>("fLong"));
            Assert.AreEqual(new float[] { 11 }, portObj.GetField<float[]>("fFloat"));
            Assert.AreEqual(new double[] { 12 }, portObj.GetField<double[]>("fDouble"));

            obj = portObj.Deserialize<PrimitiveArrays>();

            Assert.AreEqual(new byte[] { 7 }, obj.FByte);
            Assert.AreEqual(new[] { false }, obj.FBool);
            Assert.AreEqual(new short[] { 8 }, obj.FShort);
            Assert.AreEqual(new[] { 'b' }, obj.FChar);
            Assert.AreEqual(new[] { 9 }, obj.FInt);
            Assert.AreEqual(new long[] { 10 }, obj.FLong);
            Assert.AreEqual(new float[] { 11 }, obj.FFloat);
            Assert.AreEqual(new double[] { 12 }, obj.FDouble);
        }

        /// <summary>
        /// Test non-primitive fields and their array counterparts.
        /// </summary>
        [Test]
        public void TestStringDateGuidEnum()
        {
            DateTime? nDate = DateTime.Now;

            Guid? nGuid = Guid.NewGuid();

            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(StringDateGuidEnum))
                .SetField("fStr", "str")
                .SetField("fNDate", nDate)
                .SetGuidField("fNGuid", nGuid)
                .SetField("fEnum", TestEnum.One)
                .SetField("fStrArr", new[] { "str" })
                .SetArrayField("fDateArr", new[] { nDate })
                .SetGuidArrayField("fGuidArr", new[] { nGuid })
                .SetField("fEnumArr", new[] { TestEnum.One })
                .SetHashCode(100)
                .Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(StringDateGuidEnum).Name, meta.TypeName);

            Assert.AreEqual(8, meta.Fields.Count);

            Assert.AreEqual(PortableTypeNames.TypeNameString, meta.GetFieldTypeName("fStr"));
            Assert.AreEqual(PortableTypeNames.TypeNameObject, meta.GetFieldTypeName("fNDate"));
            Assert.AreEqual(PortableTypeNames.TypeNameGuid, meta.GetFieldTypeName("fNGuid"));
            Assert.AreEqual(PortableTypeNames.TypeNameEnum, meta.GetFieldTypeName("fEnum"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayString, meta.GetFieldTypeName("fStrArr"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("fDateArr"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayGuid, meta.GetFieldTypeName("fGuidArr"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayEnum, meta.GetFieldTypeName("fEnumArr"));

            Assert.AreEqual("str", portObj.GetField<string>("fStr"));
            Assert.AreEqual(nDate, portObj.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nGuid, portObj.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.One, portObj.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] { "str" }, portObj.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] { nDate }, portObj.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] { nGuid }, portObj.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] { TestEnum.One }, portObj.GetField<TestEnum[]>("fEnumArr"));

            StringDateGuidEnum obj = portObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str", obj.FStr);
            Assert.AreEqual(nDate, obj.FnDate);
            Assert.AreEqual(nGuid, obj.FnGuid);
            Assert.AreEqual(TestEnum.One, obj.FEnum);
            Assert.AreEqual(new[] { "str" }, obj.FStrArr);
            Assert.AreEqual(new[] { nDate }, obj.FDateArr);
            Assert.AreEqual(new[] { nGuid }, obj.FGuidArr);
            Assert.AreEqual(new[] { TestEnum.One }, obj.FEnumArr);

            // Check builder field caching.
            var builder = _grid.GetPortables().GetBuilder(portObj);

            Assert.AreEqual("str", builder.GetField<string>("fStr"));
            Assert.AreEqual(nDate, builder.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nGuid, builder.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.One, builder.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] { "str" }, builder.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] { nDate }, builder.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] { nGuid }, builder.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] { TestEnum.One }, builder.GetField<TestEnum[]>("fEnumArr"));

            // Check reassemble.
            portObj = builder.Build();

            Assert.AreEqual("str", portObj.GetField<string>("fStr"));
            Assert.AreEqual(nDate, portObj.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nGuid, portObj.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.One, portObj.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] { "str" }, portObj.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] { nDate }, portObj.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] { nGuid }, portObj.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] { TestEnum.One }, portObj.GetField<TestEnum[]>("fEnumArr"));

            obj = portObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str", obj.FStr);
            Assert.AreEqual(nDate, obj.FnDate);
            Assert.AreEqual(nGuid, obj.FnGuid);
            Assert.AreEqual(TestEnum.One, obj.FEnum);
            Assert.AreEqual(new[] { "str" }, obj.FStrArr);
            Assert.AreEqual(new[] { nDate }, obj.FDateArr);
            Assert.AreEqual(new[] { nGuid }, obj.FGuidArr);
            Assert.AreEqual(new[] { TestEnum.One }, obj.FEnumArr);

            // Overwrite.
            nDate = DateTime.Now.ToUniversalTime();
            nGuid = Guid.NewGuid();

            portObj = builder
                .SetField("fStr", "str2")
                .SetTimestampField("fNDate", nDate)
                .SetField("fNGuid", nGuid)
                .SetField("fEnum", TestEnum.Two)
                .SetField("fStrArr", new[] { "str2" })
                .SetArrayField("fDateArr", new[] { nDate })
                .SetField("fGuidArr", new[] { nGuid })
                .SetField("fEnumArr", new[] { TestEnum.Two })
                .SetHashCode(200)
                .Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            Assert.AreEqual("str2", portObj.GetField<string>("fStr"));
            Assert.AreEqual(nDate, portObj.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nGuid, portObj.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.Two, portObj.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] { "str2" }, portObj.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] { nDate }, portObj.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] { nGuid }, portObj.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] { TestEnum.Two }, portObj.GetField<TestEnum[]>("fEnumArr"));

            obj = portObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str2", obj.FStr);
            Assert.AreEqual(nDate, obj.FnDate);
            Assert.AreEqual(nGuid, obj.FnGuid);
            Assert.AreEqual(TestEnum.Two, obj.FEnum);
            Assert.AreEqual(new[] { "str2" }, obj.FStrArr);
            Assert.AreEqual(new[] { nDate }, obj.FDateArr);
            Assert.AreEqual(new[] { nGuid }, obj.FGuidArr);
            Assert.AreEqual(new[] { TestEnum.Two }, obj.FEnumArr);
        }

        /// <summary>
        /// Test arrays.
        /// </summary>
        [Test]
        public void TestCompositeArray()
        {
            // 1. Test simple array.
            object[] inArr = { new CompositeInner(1) };

            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(CompositeArray)).SetHashCode(100)
                .SetField("inArr", inArr).Build();

            IPortableMetadata meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(CompositeArray).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("inArr"));

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableObject[] portInArr = portObj.GetField<object[]>("inArr").Cast<IPortableObject>().ToArray();

            Assert.AreEqual(1, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].GetField<int>("val"));

            CompositeArray arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(1, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner) arr.InArr[0]).Val);

            // 2. Test addition to array.
            portObj = _grid.GetPortables().GetBuilder(portObj).SetHashCode(200)
                .SetField("inArr", new object[] { portInArr[0], null }).Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            portInArr = portObj.GetField<object[]>("inArr").Cast<IPortableObject>().ToArray();

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].GetField<int>("val"));
            Assert.IsNull(portInArr[1]);

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner) arr.InArr[0]).Val);
            Assert.IsNull(arr.InArr[1]);

            portInArr[1] = _grid.GetPortables().GetBuilder(typeof(CompositeInner)).SetField("val", 2).Build();

            portObj = _grid.GetPortables().GetBuilder(portObj).SetHashCode(300)
                .SetField("inArr", portInArr.OfType<object>().ToArray()).Build();

            Assert.AreEqual(300, portObj.GetHashCode());

            portInArr = portObj.GetField<object[]>("inArr").Cast<IPortableObject>().ToArray();

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].GetField<int>("val"));
            Assert.AreEqual(2, portInArr[1].GetField<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[0]).Val);
            Assert.AreEqual(2, ((CompositeInner)arr.InArr[1]).Val);

            // 3. Test top-level handle inversion.
            CompositeInner inner = new CompositeInner(1);

            inArr = new object[] { inner, inner };

            portObj = _grid.GetPortables().GetBuilder(typeof(CompositeArray)).SetHashCode(100)
                .SetField("inArr", inArr).Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            portInArr = portObj.GetField<object[]>("inArr").Cast<IPortableObject>().ToArray();

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].GetField<int>("val"));
            Assert.AreEqual(1, portInArr[1].GetField<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[0]).Val);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[1]).Val);

            portInArr[0] = _grid.GetPortables().GetBuilder(typeof(CompositeInner)).SetField("val", 2).Build();

            portObj = _grid.GetPortables().GetBuilder(portObj).SetHashCode(200)
                .SetField("inArr", portInArr.ToArray<object>()).Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            portInArr = portObj.GetField<object[]>("inArr").Cast<IPortableObject>().ToArray();

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(2, portInArr[0].GetField<int>("val"));
            Assert.AreEqual(1, portInArr[1].GetField<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(2, ((CompositeInner)arr.InArr[0]).Val);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[1]).Val);

            // 4. Test nested object handle inversion.
            CompositeOuter[] outArr = { new CompositeOuter(inner), new CompositeOuter(inner) };

            portObj = _grid.GetPortables().GetBuilder(typeof(CompositeArray)).SetHashCode(100)
                .SetField("outArr", outArr.ToArray<object>()).Build();

            meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(CompositeArray).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("inArr"));
            Assert.AreEqual(PortableTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("outArr"));

            Assert.AreEqual(100, portObj.GetHashCode());

            var portOutArr = portObj.GetField<object[]>("outArr").Cast<IPortableObject>().ToArray();

            Assert.AreEqual(2, portOutArr.Length);
            Assert.AreEqual(1, portOutArr[0].GetField<IPortableObject>("inner").GetField<int>("val"));
            Assert.AreEqual(1, portOutArr[1].GetField<IPortableObject>("inner").GetField<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.InArr);
            Assert.AreEqual(2, arr.OutArr.Length);
            Assert.AreEqual(1, ((CompositeOuter) arr.OutArr[0]).Inner.Val);
            Assert.AreEqual(1, ((CompositeOuter) arr.OutArr[0]).Inner.Val);

            portOutArr[0] = _grid.GetPortables().GetBuilder(typeof(CompositeOuter))
                .SetField("inner", new CompositeInner(2)).Build();

            portObj = _grid.GetPortables().GetBuilder(portObj).SetHashCode(200)
                .SetField("outArr", portOutArr.ToArray<object>()).Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            portInArr = portObj.GetField<object[]>("outArr").Cast<IPortableObject>().ToArray();

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(2, portOutArr[0].GetField<IPortableObject>("inner").GetField<int>("val"));
            Assert.AreEqual(1, portOutArr[1].GetField<IPortableObject>("inner").GetField<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.InArr);
            Assert.AreEqual(2, arr.OutArr.Length);
            Assert.AreEqual(2, ((CompositeOuter)arr.OutArr[0]).Inner.Val);
            Assert.AreEqual(1, ((CompositeOuter)arr.OutArr[1]).Inner.Val);
        }

        /// <summary>
        /// Test container types other than array.
        /// </summary>
        [Test]
        public void TestCompositeContainer()
        {
            ArrayList col = new ArrayList();
            IDictionary dict = new Hashtable();

            col.Add(new CompositeInner(1));
            dict[3] = new CompositeInner(3);

            IPortableObject portObj = _grid.GetPortables().GetBuilder(typeof(CompositeContainer)).SetHashCode(100)
                .SetCollectionField("col", col)
                .SetDictionaryField("dict", dict).Build();

            // 1. Check meta.
            IPortableMetadata meta = portObj.GetMetadata();

            Assert.AreEqual(typeof(CompositeContainer).Name, meta.TypeName);

            Assert.AreEqual(2, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TypeNameCollection, meta.GetFieldTypeName("col"));
            Assert.AreEqual(PortableTypeNames.TypeNameMap, meta.GetFieldTypeName("dict"));

            // 2. Check in portable form.
            Assert.AreEqual(1, portObj.GetField<ICollection>("col").Count);
            Assert.AreEqual(1, portObj.GetField<ICollection>("col").OfType<IPortableObject>().First()
                .GetField<int>("val"));

            Assert.AreEqual(1, portObj.GetField<IDictionary>("dict").Count);
            Assert.AreEqual(3, ((IPortableObject) portObj.GetField<IDictionary>("dict")[3]).GetField<int>("val"));

            // 3. Check in deserialized form.
            CompositeContainer obj = portObj.Deserialize<CompositeContainer>();

            Assert.AreEqual(1, obj.Col.Count);
            Assert.AreEqual(1, obj.Col.OfType<CompositeInner>().First().Val);

            Assert.AreEqual(1, obj.Dict.Count);
            Assert.AreEqual(3, ((CompositeInner) obj.Dict[3]).Val);
        }

        /// <summary>
        /// Ensure that raw data is not lost during build.
        /// </summary>
        [Test]
        public void TestRawData()
        {
            var raw = new WithRaw
            {
                A = 1,
                B = 2
            };

            var portObj = _marsh.Unmarshal<IPortableObject>(_marsh.Marshal(raw), PortableMode.ForcePortable);

            raw = portObj.Deserialize<WithRaw>();

            Assert.AreEqual(1, raw.A);
            Assert.AreEqual(2, raw.B);

            IPortableObject newPortObj = _grid.GetPortables().GetBuilder(portObj).SetField("a", 3).Build();

            raw = newPortObj.Deserialize<WithRaw>();

            Assert.AreEqual(3, raw.A);
            Assert.AreEqual(2, raw.B);
        }

        /// <summary>
        /// Test nested objects.
        /// </summary>
        [Test]
        public void TestNested()
        {
            // 1. Create from scratch.
            IPortableBuilder builder = _grid.GetPortables().GetBuilder(typeof(NestedOuter));

            NestedInner inner1 = new NestedInner {Val = 1};
            builder.SetField("inner1", inner1);

            IPortableObject outerPortObj = builder.Build();

            IPortableMetadata meta = outerPortObj.GetMetadata();

            Assert.AreEqual(typeof(NestedOuter).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TypeNameObject, meta.GetFieldTypeName("inner1"));

            IPortableObject innerPortObj1 = outerPortObj.GetField<IPortableObject>("inner1");

            IPortableMetadata innerMeta = innerPortObj1.GetMetadata();

            Assert.AreEqual(typeof(NestedInner).Name, innerMeta.TypeName);
            Assert.AreEqual(1, innerMeta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TypeNameInt, innerMeta.GetFieldTypeName("Val"));

            inner1 = innerPortObj1.Deserialize<NestedInner>();

            Assert.AreEqual(1, inner1.Val);

            NestedOuter outer = outerPortObj.Deserialize<NestedOuter>();
            Assert.AreEqual(outer.Inner1.Val, 1);
            Assert.IsNull(outer.Inner2);

            // 2. Add another field over existing portable object.
            builder = _grid.GetPortables().GetBuilder(outerPortObj);

            NestedInner inner2 = new NestedInner {Val = 2};
            builder.SetField("inner2", inner2);

            outerPortObj = builder.Build();

            outer = outerPortObj.Deserialize<NestedOuter>();
            Assert.AreEqual(1, outer.Inner1.Val);
            Assert.AreEqual(2, outer.Inner2.Val);

            // 3. Try setting inner object in portable form.
            innerPortObj1 = _grid.GetPortables().GetBuilder(innerPortObj1).SetField("val", 3).Build();

            inner1 = innerPortObj1.Deserialize<NestedInner>();

            Assert.AreEqual(3, inner1.Val);

            outerPortObj = _grid.GetPortables().GetBuilder(outerPortObj).SetField<object>("inner1", innerPortObj1).Build();

            outer = outerPortObj.Deserialize<NestedOuter>();
            Assert.AreEqual(3, outer.Inner1.Val);
            Assert.AreEqual(2, outer.Inner2.Val);
        }

        /// <summary>
        /// Test handle migration.
        /// </summary>
        [Test]
        public void TestHandleMigration()
        {
            // 1. Simple comparison of results.
            MigrationInner inner = new MigrationInner {Val = 1};

            MigrationOuter outer = new MigrationOuter
            {
                Inner1 = inner,
                Inner2 = inner
            };

            byte[] outerBytes = _marsh.Marshal(outer);

            IPortableBuilder builder = _grid.GetPortables().GetBuilder(typeof(MigrationOuter));

            builder.SetHashCode(outer.GetHashCode());

            builder.SetField<object>("inner1", inner);
            builder.SetField<object>("inner2", inner);

            PortableUserObject portOuter = (PortableUserObject) builder.Build();

            byte[] portOuterBytes = new byte[outerBytes.Length];

            Buffer.BlockCopy(portOuter.Data, 0, portOuterBytes, 0, portOuterBytes.Length);

            Assert.AreEqual(outerBytes, portOuterBytes);

            // 2. Change the first inner object so that the handle must migrate.
            MigrationInner inner1 = new MigrationInner {Val = 2};

            IPortableObject portOuterMigrated =
                _grid.GetPortables().GetBuilder(portOuter).SetField<object>("inner1", inner1).Build();

            MigrationOuter outerMigrated = portOuterMigrated.Deserialize<MigrationOuter>();

            Assert.AreEqual(2, outerMigrated.Inner1.Val);
            Assert.AreEqual(1, outerMigrated.Inner2.Val);

            // 3. Change the first value using serialized form.
            IPortableObject inner1Port =
                _grid.GetPortables().GetBuilder(typeof(MigrationInner)).SetField("val", 2).Build();

            portOuterMigrated =
                _grid.GetPortables().GetBuilder(portOuter).SetField<object>("inner1", inner1Port).Build();

            outerMigrated = portOuterMigrated.Deserialize<MigrationOuter>();

            Assert.AreEqual(2, outerMigrated.Inner1.Val);
            Assert.AreEqual(1, outerMigrated.Inner2.Val);
        }

        /// <summary>
        /// Test handle inversion.
        /// </summary>
        [Test]
        public void TestHandleInversion()
        {
            InversionInner inner = new InversionInner();
            InversionOuter outer = new InversionOuter();

            inner.Outer = outer;
            outer.Inner = inner;

            byte[] rawOuter = _marsh.Marshal(outer);

            IPortableObject portOuter = _marsh.Unmarshal<IPortableObject>(rawOuter, PortableMode.ForcePortable);
            IPortableObject portInner = portOuter.GetField<IPortableObject>("inner");

            // 1. Ensure that inner object can be deserialized after build.
            IPortableObject portInnerNew = _grid.GetPortables().GetBuilder(portInner).Build();

            InversionInner innerNew = portInnerNew.Deserialize<InversionInner>();

            Assert.AreSame(innerNew, innerNew.Outer.Inner);

            // 2. Ensure that portable object with external dependencies could be added to builder.
            IPortableObject portOuterNew =
                _grid.GetPortables().GetBuilder(typeof(InversionOuter)).SetField<object>("inner", portInner).Build();

            InversionOuter outerNew = portOuterNew.Deserialize<InversionOuter>();

            Assert.AreNotSame(outerNew, outerNew.Inner.Outer);
            Assert.AreSame(outerNew.Inner, outerNew.Inner.Outer.Inner);
        }

        /// <summary>
        /// Test build multiple objects.
        /// </summary>
        [Test]
        public void TestBuildMultiple()
        {
            IPortableBuilder builder = _grid.GetPortables().GetBuilder(typeof(Primitives));

            builder.SetField<byte>("fByte", 1).SetField("fBool", true);

            IPortableObject po1 = builder.Build();
            IPortableObject po2 = builder.Build();

            Assert.AreEqual(1, po1.GetField<byte>("fByte"));
            Assert.AreEqual(true, po1.GetField<bool>("fBool"));

            Assert.AreEqual(1, po2.GetField<byte>("fByte"));
            Assert.AreEqual(true, po2.GetField<bool>("fBool"));

            builder.SetField<byte>("fByte", 2);

            IPortableObject po3 = builder.Build();

            Assert.AreEqual(1, po1.GetField<byte>("fByte"));
            Assert.AreEqual(true, po1.GetField<bool>("fBool"));

            Assert.AreEqual(1, po2.GetField<byte>("fByte"));
            Assert.AreEqual(true, po2.GetField<bool>("fBool"));

            Assert.AreEqual(2, po3.GetField<byte>("fByte"));
            Assert.AreEqual(true, po2.GetField<bool>("fBool"));

            builder = _grid.GetPortables().GetBuilder(po1);

            builder.SetField<byte>("fByte", 10);

            po1 = builder.Build();
            po2 = builder.Build();

            builder.SetField<byte>("fByte", 20);

            po3 = builder.Build();

            Assert.AreEqual(10, po1.GetField<byte>("fByte"));
            Assert.AreEqual(true, po1.GetField<bool>("fBool"));

            Assert.AreEqual(10, po2.GetField<byte>("fByte"));
            Assert.AreEqual(true, po2.GetField<bool>("fBool"));

            Assert.AreEqual(20, po3.GetField<byte>("fByte"));
            Assert.AreEqual(true, po3.GetField<bool>("fBool"));
        }

        /// <summary>
        /// Tests type id method.
        /// </summary>
        [Test]
        public void TestTypeId()
        {
            Assert.Throws<ArgumentException>(() => _grid.GetPortables().GetTypeId(null));

            Assert.AreEqual(IdMapper.TestTypeId, _grid.GetPortables().GetTypeId(IdMapper.TestTypeName));
            
            Assert.AreEqual(PortableUtils.GetStringHashCode("someTypeName"), _grid.GetPortables().GetTypeId("someTypeName"));
        }

        /// <summary>
        /// Tests metadata methods.
        /// </summary>
        [Test]
        public void TestMetadata()
        {
            // Populate metadata
            var portables = _grid.GetPortables();

            portables.ToPortable<IPortableObject>(new DecimalHolder());

            // All meta
            var allMetas = portables.GetMetadata();

            var decimalMeta = allMetas.Single(x => x.TypeName == "DecimalHolder");

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);

            // By type
            decimalMeta = portables.GetMetadata(typeof (DecimalHolder));

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);
            
            // By type id
            decimalMeta = portables.GetMetadata(portables.GetTypeId("DecimalHolder"));

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);

            // By type name
            decimalMeta = portables.GetMetadata("DecimalHolder");

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);
        }

        /// <summary>
        /// Create portable type configuration with disabled metadata.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <returns>Configuration.</returns>
        private static PortableTypeConfiguration TypeConfigurationNoMeta(Type typ)
        {
            return new PortableTypeConfiguration(typ);
        }
    }

    /// <summary>
    /// Empty portable class.
    /// </summary>
    public class Empty
    {
        // No-op.
    }

    /// <summary>
    /// Empty portable class with no metadata.
    /// </summary>
    public class EmptyNoMeta
    {
        // No-op.
    }

    /// <summary>
    /// Portable with primitive fields.
    /// </summary>
    public class Primitives
    {
        public byte FByte;
        public bool FBool;
        public short FShort;
        public char FChar;
        public int FInt;
        public long FLong;
        public float FFloat;
        public double FDouble;
    }

    /// <summary>
    /// Portable with primitive array fields.
    /// </summary>
    public class PrimitiveArrays
    {
        public byte[] FByte;
        public bool[] FBool;
        public short[] FShort;
        public char[] FChar;
        public int[] FInt;
        public long[] FLong;
        public float[] FFloat;
        public double[] FDouble;
    }

    /// <summary>
    /// Portable having strings, dates, Guids and enums.
    /// </summary>
    public class StringDateGuidEnum
    {
        public string FStr;
        public DateTime? FnDate;
        public Guid? FnGuid;
        public TestEnum FEnum;

        public string[] FStrArr;
        public DateTime?[] FDateArr;
        public Guid?[] FGuidArr;
        public TestEnum[] FEnumArr;
    }

    /// <summary>
    /// Enumeration.
    /// </summary>
    public enum TestEnum
    {
        One, Two
    }

    /// <summary>
    /// Portable with raw data.
    /// </summary>
    public class WithRaw : IPortableMarshalAware
    {
        public int A;
        public int B;

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteInt("a", A);
            writer.GetRawWriter().WriteInt(B);
        }

        /** <inheritDoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            A = reader.ReadInt("a");
            B = reader.GetRawReader().ReadInt();
        }
    }

    /// <summary>
    /// Empty class for metadata overwrite test.
    /// </summary>
    public class MetaOverwrite
    {
        // No-op.
    }

    /// <summary>
    /// Nested outer object.
    /// </summary>
    public class NestedOuter
    {
        public NestedInner Inner1;
        public NestedInner Inner2;
    }

    /// <summary>
    /// Nested inner object.
    /// </summary>
    public class NestedInner
    {
        public int Val;
    }

    /// <summary>
    /// Outer object for handle migration test.
    /// </summary>
    public class MigrationOuter
    {
        public MigrationInner Inner1;
        public MigrationInner Inner2;
    }

    /// <summary>
    /// Inner object for handle migration test.
    /// </summary>
    public class MigrationInner
    {
        public int Val;
    }

    /// <summary>
    /// Outer object for handle inversion test.
    /// </summary>
    public class InversionOuter
    {
        public InversionInner Inner;
    }

    /// <summary>
    /// Inner object for handle inversion test.
    /// </summary>
    public class InversionInner
    {
        public InversionOuter Outer;
    }

    /// <summary>
    /// Object for composite array tests.
    /// </summary>
    public class CompositeArray
    {
        public object[] InArr;
        public object[] OutArr;
    }

    /// <summary>
    /// Object for composite collection/dictionary tests.
    /// </summary>
    public class CompositeContainer
    {
        public ICollection Col;
        public IDictionary Dict;
    }

    /// <summary>
    /// OUter object for composite structures test.
    /// </summary>
    public class CompositeOuter
    {
        public CompositeInner Inner;

        public CompositeOuter()
        {
            // No-op.
        }

        public CompositeOuter(CompositeInner inner)
        {
            Inner = inner;
        }
    }

    /// <summary>
    /// Inner object for composite structures test.
    /// </summary>
    public class CompositeInner
    {
        public int Val;

        public CompositeInner()
        {
            // No-op.
        }

        public CompositeInner(int val)
        {
            Val = val;
        }
    }

    /// <summary>
    /// Type to test "ToPortable()" logic.
    /// </summary>
    public class ToPortable
    {
        public int Val;

        public ToPortable(int val)
        {
            Val = val;
        }
    }

    /// <summary>
    /// Type to test "ToPortable()" logic with metadata disabled.
    /// </summary>
    public class ToPortableNoMeta
    {
        public int Val;

        public ToPortableNoMeta(int val)
        {
            Val = val;
        }
    }

    /// <summary>
    /// Type to test removal.
    /// </summary>
    public class Remove
    {
        public object Val;
        public RemoveInner Val2;
    }

    /// <summary>
    /// Inner type to test removal.
    /// </summary>
    public class RemoveInner
    {
        /** */
        public int Val;

        /// <summary>
        ///
        /// </summary>
        /// <param name="val"></param>
        public RemoveInner(int val)
        {
            Val = val;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderInBuilderOuter
    {
        /** */
        public BuilderInBuilderInner Inner;

        /** */
        public BuilderInBuilderInner Inner2;
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderInBuilderInner
    {
        /** */
        public BuilderInBuilderOuter Outer;
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderCollection
    {
        /** */
        public readonly ArrayList Col;

        /// <summary>
        ///
        /// </summary>
        /// <param name="col"></param>
        public BuilderCollection(ArrayList col)
        {
            Col = col;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderCollectionItem
    {
        /** */
        public int Val;

        /// <summary>
        ///
        /// </summary>
        /// <param name="val"></param>
        public BuilderCollectionItem(int val)
        {
            Val = val;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class DecimalHolder
    {
        /** */
        public decimal Val;

        /** */
        public decimal?[] ValArr;
    }

    /// <summary>
    /// Test id mapper.
    /// </summary>
    public class IdMapper : IPortableIdMapper
    {
        /** */
        public const string TestTypeName = "IdMapperTestType";

        /** */
        public const int TestTypeId = -65537;

        /** <inheritdoc /> */
        public int GetTypeId(string typeName)
        {
            return typeName == TestTypeName ? TestTypeId : 0;
        }

        /** <inheritdoc /> */
        public int GetFieldId(int typeId, string fieldName)
        {
            return 0;
        }
    }
}
