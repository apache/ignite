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
    public class GridPortableApiSelfTest
    {
        /** Undefined type: Empty. */
        private const string TYPE_EMPTY = "EmptyUndefined";

        /** Grid. */
        private GridImpl grid;

        /** Marshaller. */
        private PortableMarshaller marsh;

        /// <summary>
        /// Set up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            GridTestUtils.KillProcesses();

            var cfg = new GridConfiguration
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
                        new PortableTypeConfiguration(TYPE_EMPTY),
                        TypeConfigurationNoMeta(typeof (EmptyNoMeta)),
                        TypeConfigurationNoMeta(typeof (ToPortableNoMeta))
                    },
                    DefaultIdMapper = new IdMapper()
                },
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
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

            grid = (GridImpl) GridFactory.Start(cfg);

            marsh = grid.Marshaller;
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void TearDown()
        {
            if (grid != null)
                GridFactory.Stop(grid.Name, true);

            grid = null;
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
            IPortableObject portObj1 = grid.Portables().Builder(typeName1).SetField(field1, 1).Build();

            Assert.AreEqual(typeName1, portObj1.Metadata().TypeName);
            Assert.AreEqual(1, portObj1.Metadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.Metadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, portObj1.Metadata().FieldTypeName(field1));

            Assert.AreEqual(1, portObj1.Field<int>(field1));

            // 2. Ensure that object can be unmarshalled without deserialization.
            byte[] data = ((PortableUserObject) portObj1).Data;

            portObj1 = grid.Marshaller.Unmarshal<IPortableObject>(data, PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(typeName1, portObj1.Metadata().TypeName);
            Assert.AreEqual(1, portObj1.Metadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.Metadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, portObj1.Metadata().FieldTypeName(field1));

            Assert.AreEqual(1, portObj1.Field<int>(field1));

            // 3. Ensure that we can nest one anonymous object inside another
            IPortableObject portObj2 =
                grid.Portables().Builder(typeName2).SetField(field2, portObj1).Build();

            Assert.AreEqual(typeName2, portObj2.Metadata().TypeName);
            Assert.AreEqual(1, portObj2.Metadata().Fields.Count);
            Assert.AreEqual(field2, portObj2.Metadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, portObj2.Metadata().FieldTypeName(field2));

            portObj1 = portObj2.Field<IPortableObject>(field2);

            Assert.AreEqual(typeName1, portObj1.Metadata().TypeName);
            Assert.AreEqual(1, portObj1.Metadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.Metadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, portObj1.Metadata().FieldTypeName(field1));

            Assert.AreEqual(1, portObj1.Field<int>(field1));

            // 4. Ensure that we can unmarshal object with other nested object.
            data = ((PortableUserObject) portObj2).Data;

            portObj2 = grid.Marshaller.Unmarshal<IPortableObject>(data, PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(typeName2, portObj2.Metadata().TypeName);
            Assert.AreEqual(1, portObj2.Metadata().Fields.Count);
            Assert.AreEqual(field2, portObj2.Metadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, portObj2.Metadata().FieldTypeName(field2));

            portObj1 = portObj2.Field<IPortableObject>(field2);

            Assert.AreEqual(typeName1, portObj1.Metadata().TypeName);
            Assert.AreEqual(1, portObj1.Metadata().Fields.Count);
            Assert.AreEqual(field1, portObj1.Metadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, portObj1.Metadata().FieldTypeName(field1));

            Assert.AreEqual(1, portObj1.Field<int>(field1));
        }

        /// <summary>
        /// Test "ToPortable()" method.
        /// </summary>
        [Test]
        public void TestToPortable()
        {
            DateTime date = DateTime.Now.ToUniversalTime();
            Guid guid = Guid.NewGuid();

            IPortables api = grid.Portables();

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
            Assert.AreEqual(TestEnum.ONE, api.ToPortable<TestEnum>(TestEnum.ONE));

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
            Assert.AreEqual(new[] { TestEnum.ONE }, api.ToPortable<TestEnum[]>(new[] { TestEnum.ONE }));

            // 4. Objects.
            IPortableObject portObj = api.ToPortable<IPortableObject>(new ToPortable(1));

            Assert.AreEqual(typeof(ToPortable).Name, portObj.Metadata().TypeName);
            Assert.AreEqual(1, portObj.Metadata().Fields.Count);
            Assert.AreEqual("val", portObj.Metadata().Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, portObj.Metadata().FieldTypeName("val"));

            Assert.AreEqual(1, portObj.Field<int>("val"));
            Assert.AreEqual(1, portObj.Deserialize<ToPortable>().val);

            portObj = api.ToPortable<IPortableObject>(new ToPortableNoMeta(1));

            Assert.AreEqual(0, portObj.Metadata().Fields.Count);

            Assert.AreEqual(1, portObj.Field<int>("val"));
            Assert.AreEqual(1, portObj.Deserialize<ToPortableNoMeta>().val);

            // 5. Object array.
            IPortableObject[] portObjArr = api.ToPortable<IPortableObject[]>(new[] { new ToPortable(1) });

            Assert.AreEqual(1, portObjArr.Length);
            Assert.AreEqual(1, portObjArr[0].Field<int>("val"));
            Assert.AreEqual(1, portObjArr[0].Deserialize<ToPortable>().val);
        }

        /// <summary>
        /// Test builder field remove logic.
        /// </summary>
        [Test]
        public void TestRemove()
        {
            // Create empty object.
            IPortableObject portObj = grid.Portables().Builder(typeof(Remove)).Build();

            Assert.IsNull(portObj.Field<object>("val"));
            Assert.IsNull(portObj.Deserialize<Remove>().val);

            IPortableMetadata meta = portObj.Metadata();

            Assert.AreEqual(typeof(Remove).Name, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);

            // Populate it with field.
            IPortableBuilder builder = grid.Portables().Builder(portObj);

            Assert.IsNull(builder.GetField<object>("val"));

            object val = 1;

            builder.SetField("val", val);

            Assert.AreEqual(val, builder.GetField<object>("val"));

            portObj = builder.Build();

            Assert.AreEqual(val, portObj.Field<object>("val"));
            Assert.AreEqual(val, portObj.Deserialize<Remove>().val);

            meta = portObj.Metadata();

            Assert.AreEqual(typeof(Remove).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("val", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, meta.FieldTypeName("val"));

            // Perform field remove.
            builder = grid.Portables().Builder(portObj);

            Assert.AreEqual(val, builder.GetField<object>("val"));

            builder.RemoveField("val");
            Assert.IsNull(builder.GetField<object>("val"));

            builder.SetField("val", val);
            Assert.AreEqual(val, builder.GetField<object>("val"));

            builder.RemoveField("val");
            Assert.IsNull(builder.GetField<object>("val"));

            portObj = builder.Build();

            Assert.IsNull(portObj.Field<object>("val"));
            Assert.IsNull(portObj.Deserialize<Remove>().val);

            // Test correct removal of field being referenced by handle somewhere else.
            RemoveInner inner = new RemoveInner(2);

            portObj = grid.Portables().Builder(typeof(Remove))
                .SetField("val", inner)
                .SetField("val2", inner)
                .Build();

            portObj = grid.Portables().Builder(portObj).RemoveField("val").Build();

            Remove obj = portObj.Deserialize<Remove>();

            Assert.IsNull(obj.val);
            Assert.AreEqual(2, obj.val2.val);
        }

        /// <summary>
        /// Test builder-in-builder scenario.
        /// </summary>
        [Test]
        public void TestBuilderInBuilder()
        {
            // Test different builders assembly.
            IPortableBuilder builderOuter = grid.Portables().Builder(typeof(BuilderInBuilderOuter));
            IPortableBuilder builderInner = grid.Portables().Builder(typeof(BuilderInBuilderInner));

            builderOuter.SetField<object>("inner", builderInner);
            builderInner.SetField<object>("outer", builderOuter);

            IPortableObject outerPortObj = builderOuter.Build();

            IPortableMetadata meta = outerPortObj.Metadata();

            Assert.AreEqual(typeof(BuilderInBuilderOuter).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("inner", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, meta.FieldTypeName("inner"));

            IPortableObject innerPortObj = outerPortObj.Field<IPortableObject>("inner");

            meta = innerPortObj.Metadata();

            Assert.AreEqual(typeof(BuilderInBuilderInner).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("outer", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, meta.FieldTypeName("outer"));

            BuilderInBuilderOuter outer = outerPortObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer, outer.inner.outer);

            // Test same builders assembly.
            innerPortObj = grid.Portables().Builder(typeof(BuilderInBuilderInner)).Build();

            outerPortObj = grid.Portables().Builder(typeof(BuilderInBuilderOuter))
                .SetField("inner", innerPortObj)
                .SetField("inner2", innerPortObj)
                .Build();

            meta = outerPortObj.Metadata();

            Assert.AreEqual(typeof(BuilderInBuilderOuter).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.IsTrue(meta.Fields.Contains("inner"));
            Assert.IsTrue(meta.Fields.Contains("inner2"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, meta.FieldTypeName("inner"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, meta.FieldTypeName("inner2"));

            outer = outerPortObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer.inner, outer.inner2);

            builderOuter = grid.Portables().Builder(outerPortObj);
            IPortableBuilder builderInner2 = builderOuter.GetField<IPortableBuilder>("inner2");

            builderInner2.SetField("outer", builderOuter);

            outerPortObj = builderOuter.Build();

            outer = outerPortObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer, outer.inner.outer);
            Assert.AreSame(outer.inner, outer.inner2);
        }

        /// <summary>
        /// Test for decimals building.
        /// </summary>
        [Test]
        public void TestDecimals()
        {
            IPortableObject portObj = grid.Portables().Builder(typeof(DecimalHolder))
                .SetField("val", decimal.One)
                .SetField("valArr", new[] { decimal.MinusOne })
                .Build();

            IPortableMetadata meta = portObj.Metadata();

            Assert.AreEqual(typeof(DecimalHolder).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.IsTrue(meta.Fields.Contains("val"));
            Assert.IsTrue(meta.Fields.Contains("valArr"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_DECIMAL, meta.FieldTypeName("val"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_DECIMAL, meta.FieldTypeName("valArr"));

            Assert.AreEqual(decimal.One, portObj.Field<decimal>("val"));
            Assert.AreEqual(new[] { decimal.MinusOne }, portObj.Field<decimal[]>("valArr"));

            DecimalHolder obj = portObj.Deserialize<DecimalHolder>();

            Assert.AreEqual(decimal.One, obj.val);
            Assert.AreEqual(new[] { decimal.MinusOne }, obj.valArr);
        }

        /// <summary>
        /// Test for an object returning collection of builders.
        /// </summary>
        [Test]
        public void TestBuilderCollection()
        {
            // Test collection with single element.
            IPortableBuilder builderCol = grid.Portables().Builder(typeof(BuilderCollection));
            IPortableBuilder builderItem =
                grid.Portables().Builder(typeof(BuilderCollectionItem)).SetField("val", 1);

            builderCol.SetField<ICollection>("col", new List<IPortableBuilder>() { builderItem });

            IPortableObject portCol = builderCol.Build();

            IPortableMetadata meta = portCol.Metadata();

            Assert.AreEqual(typeof(BuilderCollection).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("col", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_COLLECTION, meta.FieldTypeName("col"));

            ICollection<IPortableObject> portColItems = portCol.Field<ICollection<IPortableObject>>("col");

            Assert.AreEqual(1, portColItems.Count);

            IPortableObject portItem = portColItems.First();

            meta = portItem.Metadata();

            Assert.AreEqual(typeof(BuilderCollectionItem).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("val", meta.Fields.First());
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, meta.FieldTypeName("val"));

            BuilderCollection col = portCol.Deserialize<BuilderCollection>();

            Assert.IsNotNull(col.col);
            Assert.AreEqual(1, col.col.Count);
            Assert.AreEqual(1, col.col.First().val);

            // Add more portable objects to collection.
            builderCol = grid.Portables().Builder(portCol);

            IList builderColItems = builderCol.GetField<IList>("col");

            Assert.AreEqual(1, builderColItems.Count);

            PortableBuilderImpl builderColItem = (PortableBuilderImpl) builderColItems[0];

            builderColItem.SetField("val", 2); // Change nested value.

            builderColItems.Add(builderColItem); // Add the same object to check handles.
            builderColItems.Add(builderItem); // Add item from another builder.
            builderColItems.Add(portItem); // Add item in portable form.

            portCol = builderCol.Build();

            col = portCol.Deserialize<BuilderCollection>();

            Assert.AreEqual(4, col.col.Count);

            BuilderCollectionItem item0 = col.col.ElementAt(0);
            BuilderCollectionItem item1 = col.col.ElementAt(1);
            BuilderCollectionItem item2 = col.col.ElementAt(2);
            BuilderCollectionItem item3 = col.col.ElementAt(3);

            Assert.AreEqual(2, item0.val);

            Assert.AreSame(item0, item1);
            Assert.AreNotSame(item0, item2);
            Assert.AreNotSame(item0, item3);

            Assert.AreEqual(1, item2.val);
            Assert.AreEqual(1, item3.val);

            Assert.AreNotSame(item2, item3);

            // Test handle update inside collection.
            builderCol = grid.Portables().Builder(portCol);

            builderColItems = builderCol.GetField<IList>("col");

            ((PortableBuilderImpl) builderColItems[1]).SetField("val", 3);

            portCol = builderCol.Build();

            col = portCol.Deserialize<BuilderCollection>();

            item0 = col.col.ElementAt(0);
            item1 = col.col.ElementAt(1);

            Assert.AreEqual(3, item0.val);
            Assert.AreSame(item0, item1);
        }

        /// <summary>
        /// Test build of an empty object.
        /// </summary>
        [Test]
        public void TestEmptyDefined()
        {
            IPortableObject portObj = grid.Portables().Builder(typeof(Empty)).Build();

            Assert.IsNotNull(portObj);
            Assert.AreEqual(0, portObj.GetHashCode());

            IPortableMetadata meta = portObj.Metadata();

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
            IPortableObject portObj = grid.Portables().Builder(typeof(EmptyNoMeta)).Build();

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
            IPortableObject portObj = grid.Portables().Builder(TYPE_EMPTY).Build();

            Assert.IsNotNull(portObj);
            Assert.AreEqual(0, portObj.GetHashCode());

            IPortableMetadata meta = portObj.Metadata();

            Assert.IsNotNull(meta);
            Assert.AreEqual(TYPE_EMPTY, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);
        }

        /// <summary>
        /// Test object rebuild with no changes.
        /// </summary>
        [Test]
        public void TestEmptyRebuild()
        {
            var portObj = (PortableUserObject) grid.Portables().Builder(typeof(EmptyNoMeta)).Build();

            PortableUserObject newPortObj = (PortableUserObject) grid.Portables().Builder(portObj).Build();

            Assert.AreEqual(portObj.Data, newPortObj.Data);
        }

        /// <summary>
        /// Test hash code alteration.
        /// </summary>
        [Test]
        public void TestHashCodeChange()
        {
            IPortableObject portObj = grid.Portables().Builder(typeof(EmptyNoMeta)).HashCode(100).Build();

            Assert.AreEqual(100, portObj.GetHashCode());
        }

        /// <summary>
        /// Test primitive fields setting.
        /// </summary>
        [Test]
        public void TestPrimitiveFields()
        {
            IPortableObject portObj = grid.Portables().Builder(typeof(Primitives))
                .SetField<byte>("fByte", 1)
                .SetField("fBool", true)
                .SetField<short>("fShort", 2)
                .SetField("fChar", 'a')
                .SetField("fInt", 3)
                .SetField<long>("fLong", 4)
                .SetField<float>("fFloat", 5)
                .SetField<double>("fDouble", 6)
                .HashCode(100)
                .Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableMetadata meta = portObj.Metadata();

            Assert.AreEqual(typeof(Primitives).Name, meta.TypeName);

            Assert.AreEqual(8, meta.Fields.Count);

            Assert.AreEqual(PortableTypeNames.TYPE_NAME_BYTE, meta.FieldTypeName("fByte"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_BOOL, meta.FieldTypeName("fBool"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_SHORT, meta.FieldTypeName("fShort"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_CHAR, meta.FieldTypeName("fChar"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, meta.FieldTypeName("fInt"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_LONG, meta.FieldTypeName("fLong"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_FLOAT, meta.FieldTypeName("fFloat"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_DOUBLE, meta.FieldTypeName("fDouble"));

            Assert.AreEqual(1, portObj.Field<byte>("fByte"));
            Assert.AreEqual(true, portObj.Field<bool>("fBool"));
            Assert.AreEqual(2, portObj.Field<short>("fShort"));
            Assert.AreEqual('a', portObj.Field<char>("fChar"));
            Assert.AreEqual(3, portObj.Field<int>("fInt"));
            Assert.AreEqual(4, portObj.Field<long>("fLong"));
            Assert.AreEqual(5, portObj.Field<float>("fFloat"));
            Assert.AreEqual(6, portObj.Field<double>("fDouble"));

            Primitives obj = portObj.Deserialize<Primitives>();

            Assert.AreEqual(1, obj.fByte);
            Assert.AreEqual(true, obj.fBool);
            Assert.AreEqual(2, obj.fShort);
            Assert.AreEqual('a', obj.fChar);
            Assert.AreEqual(3, obj.fInt);
            Assert.AreEqual(4, obj.fLong);
            Assert.AreEqual(5, obj.fFloat);
            Assert.AreEqual(6, obj.fDouble);

            // Overwrite.
            portObj = grid.Portables().Builder(portObj)
                .SetField<byte>("fByte", 7)
                .SetField("fBool", false)
                .SetField<short>("fShort", 8)
                .SetField("fChar", 'b')
                .SetField("fInt", 9)
                .SetField<long>("fLong", 10)
                .SetField<float>("fFloat", 11)
                .SetField<double>("fDouble", 12)
                .HashCode(200)
                .Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            Assert.AreEqual(7, portObj.Field<byte>("fByte"));
            Assert.AreEqual(false, portObj.Field<bool>("fBool"));
            Assert.AreEqual(8, portObj.Field<short>("fShort"));
            Assert.AreEqual('b', portObj.Field<char>("fChar"));
            Assert.AreEqual(9, portObj.Field<int>("fInt"));
            Assert.AreEqual(10, portObj.Field<long>("fLong"));
            Assert.AreEqual(11, portObj.Field<float>("fFloat"));
            Assert.AreEqual(12, portObj.Field<double>("fDouble"));

            obj = portObj.Deserialize<Primitives>();

            Assert.AreEqual(7, obj.fByte);
            Assert.AreEqual(false, obj.fBool);
            Assert.AreEqual(8, obj.fShort);
            Assert.AreEqual('b', obj.fChar);
            Assert.AreEqual(9, obj.fInt);
            Assert.AreEqual(10, obj.fLong);
            Assert.AreEqual(11, obj.fFloat);
            Assert.AreEqual(12, obj.fDouble);
        }

        /// <summary>
        /// Test primitive array fields setting.
        /// </summary>
        [Test]
        public void TestPrimitiveArrayFields()
        {
            IPortableObject portObj = grid.Portables().Builder(typeof(PrimitiveArrays))
                .SetField("fByte", new byte[] { 1 })
                .SetField("fBool", new[] { true })
                .SetField("fShort", new short[] { 2 })
                .SetField("fChar", new[] { 'a' })
                .SetField("fInt", new[] { 3 })
                .SetField("fLong", new long[] { 4 })
                .SetField("fFloat", new float[] { 5 })
                .SetField("fDouble", new double[] { 6 })
                .HashCode(100)
                .Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableMetadata meta = portObj.Metadata();

            Assert.AreEqual(typeof(PrimitiveArrays).Name, meta.TypeName);

            Assert.AreEqual(8, meta.Fields.Count);

            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_BYTE, meta.FieldTypeName("fByte"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_BOOL, meta.FieldTypeName("fBool"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_SHORT, meta.FieldTypeName("fShort"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_CHAR, meta.FieldTypeName("fChar"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_INT, meta.FieldTypeName("fInt"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_LONG, meta.FieldTypeName("fLong"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_FLOAT, meta.FieldTypeName("fFloat"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_DOUBLE, meta.FieldTypeName("fDouble"));

            Assert.AreEqual(new byte[] { 1 }, portObj.Field<byte[]>("fByte"));
            Assert.AreEqual(new[] { true }, portObj.Field<bool[]>("fBool"));
            Assert.AreEqual(new short[] { 2 }, portObj.Field<short[]>("fShort"));
            Assert.AreEqual(new[] { 'a' }, portObj.Field<char[]>("fChar"));
            Assert.AreEqual(new[] { 3 }, portObj.Field<int[]>("fInt"));
            Assert.AreEqual(new long[] { 4 }, portObj.Field<long[]>("fLong"));
            Assert.AreEqual(new float[] { 5 }, portObj.Field<float[]>("fFloat"));
            Assert.AreEqual(new double[] { 6 }, portObj.Field<double[]>("fDouble"));

            PrimitiveArrays obj = portObj.Deserialize<PrimitiveArrays>();

            Assert.AreEqual(new byte[] { 1 }, obj.fByte);
            Assert.AreEqual(new[] { true }, obj.fBool);
            Assert.AreEqual(new short[] { 2 }, obj.fShort);
            Assert.AreEqual(new[] { 'a' }, obj.fChar);
            Assert.AreEqual(new[] { 3 }, obj.fInt);
            Assert.AreEqual(new long[] { 4 }, obj.fLong);
            Assert.AreEqual(new float[] { 5 }, obj.fFloat);
            Assert.AreEqual(new double[] { 6 }, obj.fDouble);

            // Overwrite.
            portObj = grid.Portables().Builder(portObj)
                .SetField("fByte", new byte[] { 7 })
                .SetField("fBool", new[] { false })
                .SetField("fShort", new short[] { 8 })
                .SetField("fChar", new[] { 'b' })
                .SetField("fInt", new[] { 9 })
                .SetField("fLong", new long[] { 10 })
                .SetField("fFloat", new float[] { 11 })
                .SetField("fDouble", new double[] { 12 })
                .HashCode(200)
                .Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            Assert.AreEqual(new byte[] { 7 }, portObj.Field<byte[]>("fByte"));
            Assert.AreEqual(new[] { false }, portObj.Field<bool[]>("fBool"));
            Assert.AreEqual(new short[] { 8 }, portObj.Field<short[]>("fShort"));
            Assert.AreEqual(new[] { 'b' }, portObj.Field<char[]>("fChar"));
            Assert.AreEqual(new[] { 9 }, portObj.Field<int[]>("fInt"));
            Assert.AreEqual(new long[] { 10 }, portObj.Field<long[]>("fLong"));
            Assert.AreEqual(new float[] { 11 }, portObj.Field<float[]>("fFloat"));
            Assert.AreEqual(new double[] { 12 }, portObj.Field<double[]>("fDouble"));

            obj = portObj.Deserialize<PrimitiveArrays>();

            Assert.AreEqual(new byte[] { 7 }, obj.fByte);
            Assert.AreEqual(new[] { false }, obj.fBool);
            Assert.AreEqual(new short[] { 8 }, obj.fShort);
            Assert.AreEqual(new[] { 'b' }, obj.fChar);
            Assert.AreEqual(new[] { 9 }, obj.fInt);
            Assert.AreEqual(new long[] { 10 }, obj.fLong);
            Assert.AreEqual(new float[] { 11 }, obj.fFloat);
            Assert.AreEqual(new double[] { 12 }, obj.fDouble);
        }

        /// <summary>
        /// Test non-primitive fields and their array counterparts.
        /// </summary>
        [Test]
        public void TestStringDateGuidEnum()
        {
            DateTime date = DateTime.Now.ToUniversalTime();
            DateTime? nDate = DateTime.Now.ToUniversalTime();

            Guid guid = Guid.NewGuid();
            Guid? nGuid = Guid.NewGuid();

            IPortableObject portObj = grid.Portables().Builder(typeof(StringDateGuidEnum))
                .SetField("fStr", "str")
                .SetField("fDate", date)
                .SetField("fNDate", nDate)
                .SetField("fGuid", guid)
                .SetField("fNGuid", nGuid)
                .SetField("fEnum", TestEnum.ONE)
                .SetField("fStrArr", new[] { "str" })
                .SetField("fDateArr", new[] { nDate })
                .SetField("fGuidArr", new[] { nGuid })
                .SetField("fEnumArr", new[] { TestEnum.ONE })
                .HashCode(100)
                .Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableMetadata meta = portObj.Metadata();

            Assert.AreEqual(typeof(StringDateGuidEnum).Name, meta.TypeName);

            Assert.AreEqual(10, meta.Fields.Count);

            Assert.AreEqual(PortableTypeNames.TYPE_NAME_STRING, meta.FieldTypeName("fStr"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_DATE, meta.FieldTypeName("fDate"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_DATE, meta.FieldTypeName("fNDate"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_GUID, meta.FieldTypeName("fGuid"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_GUID, meta.FieldTypeName("fNGuid"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ENUM, meta.FieldTypeName("fEnum"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_STRING, meta.FieldTypeName("fStrArr"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_DATE, meta.FieldTypeName("fDateArr"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_GUID, meta.FieldTypeName("fGuidArr"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_ENUM, meta.FieldTypeName("fEnumArr"));

            Assert.AreEqual("str", portObj.Field<string>("fStr"));
            Assert.AreEqual(date, portObj.Field<DateTime>("fDate"));
            Assert.AreEqual(nDate, portObj.Field<DateTime?>("fNDate"));
            Assert.AreEqual(guid, portObj.Field<Guid>("fGuid"));
            Assert.AreEqual(nGuid, portObj.Field<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.ONE, portObj.Field<TestEnum>("fEnum"));
            Assert.AreEqual(new[] { "str" }, portObj.Field<string[]>("fStrArr"));
            Assert.AreEqual(new[] { nDate }, portObj.Field<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] { nGuid }, portObj.Field<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] { TestEnum.ONE }, portObj.Field<TestEnum[]>("fEnumArr"));

            StringDateGuidEnum obj = portObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str", obj.fStr);
            Assert.AreEqual(date, obj.fDate);
            Assert.AreEqual(nDate, obj.fNDate);
            Assert.AreEqual(guid, obj.fGuid);
            Assert.AreEqual(nGuid, obj.fNGuid);
            Assert.AreEqual(TestEnum.ONE, obj.fEnum);
            Assert.AreEqual(new[] { "str" }, obj.fStrArr);
            Assert.AreEqual(new[] { nDate }, obj.fDateArr);
            Assert.AreEqual(new[] { nGuid }, obj.fGuidArr);
            Assert.AreEqual(new[] { TestEnum.ONE }, obj.fEnumArr);

            // Overwrite.
            date = DateTime.Now.ToUniversalTime();
            nDate = DateTime.Now.ToUniversalTime();

            guid = Guid.NewGuid();
            nGuid = Guid.NewGuid();

            portObj = grid.Portables().Builder(typeof(StringDateGuidEnum))
                .SetField("fStr", "str2")
                .SetField("fDate", date)
                .SetField("fNDate", nDate)
                .SetField("fGuid", guid)
                .SetField("fNGuid", nGuid)
                .SetField("fEnum", TestEnum.TWO)
                .SetField("fStrArr", new[] { "str2" })
                .SetField("fDateArr", new[] { nDate })
                .SetField("fGuidArr", new[] { nGuid })
                .SetField("fEnumArr", new[] { TestEnum.TWO })
                .HashCode(200)
                .Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            Assert.AreEqual("str2", portObj.Field<string>("fStr"));
            Assert.AreEqual(date, portObj.Field<DateTime>("fDate"));
            Assert.AreEqual(nDate, portObj.Field<DateTime?>("fNDate"));
            Assert.AreEqual(guid, portObj.Field<Guid>("fGuid"));
            Assert.AreEqual(nGuid, portObj.Field<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.TWO, portObj.Field<TestEnum>("fEnum"));
            Assert.AreEqual(new[] { "str2" }, portObj.Field<string[]>("fStrArr"));
            Assert.AreEqual(new[] { nDate }, portObj.Field<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] { nGuid }, portObj.Field<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] { TestEnum.TWO }, portObj.Field<TestEnum[]>("fEnumArr"));

            obj = portObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str2", obj.fStr);
            Assert.AreEqual(date, obj.fDate);
            Assert.AreEqual(nDate, obj.fNDate);
            Assert.AreEqual(guid, obj.fGuid);
            Assert.AreEqual(nGuid, obj.fNGuid);
            Assert.AreEqual(TestEnum.TWO, obj.fEnum);
            Assert.AreEqual(new[] { "str2" }, obj.fStrArr);
            Assert.AreEqual(new[] { nDate }, obj.fDateArr);
            Assert.AreEqual(new[] { nGuid }, obj.fGuidArr);
            Assert.AreEqual(new[] { TestEnum.TWO }, obj.fEnumArr);
        }

        /// <summary>
        /// Test arrays.
        /// </summary>
        [Test]
        public void TestCompositeArray()
        {
            // 1. Test simple array.
            CompositeInner[] inArr = new[] { new CompositeInner(1) };

            IPortableObject portObj = grid.Portables().Builder(typeof(CompositeArray)).HashCode(100)
                .SetField("inArr", inArr).Build();

            IPortableMetadata meta = portObj.Metadata();

            Assert.AreEqual(typeof(CompositeArray).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_OBJECT, meta.FieldTypeName("inArr"));

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableObject[] portInArr = portObj.Field<IPortableObject[]>("inArr");

            Assert.AreEqual(1, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].Field<int>("val"));

            CompositeArray arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.outArr);
            Assert.AreEqual(1, arr.inArr.Length);
            Assert.AreEqual(1, arr.inArr[0].val);

            // 2. Test addition to array.
            portInArr = new[] { portInArr[0], null };

            portObj = grid.Portables().Builder(portObj).HashCode(200)
                .SetField("inArr", portInArr).Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            portInArr = portObj.Field<IPortableObject[]>("inArr");

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].Field<int>("val"));
            Assert.IsNull(portInArr[1]);

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.outArr);
            Assert.AreEqual(2, arr.inArr.Length);
            Assert.AreEqual(1, arr.inArr[0].val);
            Assert.IsNull(arr.inArr[1]);

            portInArr[1] = grid.Portables().Builder(typeof(CompositeInner)).SetField("val", 2).Build();

            portObj = grid.Portables().Builder(portObj).HashCode(300)
                .SetField("inArr", portInArr).Build();

            Assert.AreEqual(300, portObj.GetHashCode());

            portInArr = portObj.Field<IPortableObject[]>("inArr");

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].Field<int>("val"));
            Assert.AreEqual(2, portInArr[1].Field<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.outArr);
            Assert.AreEqual(2, arr.inArr.Length);
            Assert.AreEqual(1, arr.inArr[0].val);
            Assert.AreEqual(2, arr.inArr[1].val);

            // 3. Test top-level handle inversion.
            CompositeInner inner = new CompositeInner(1);

            inArr = new[] { inner, inner };

            portObj = grid.Portables().Builder(typeof(CompositeArray)).HashCode(100)
                .SetField("inArr", inArr).Build();

            Assert.AreEqual(100, portObj.GetHashCode());

            portInArr = portObj.Field<IPortableObject[]>("inArr");

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(1, portInArr[0].Field<int>("val"));
            Assert.AreEqual(1, portInArr[1].Field<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.outArr);
            Assert.AreEqual(2, arr.inArr.Length);
            Assert.AreEqual(1, arr.inArr[0].val);
            Assert.AreEqual(1, arr.inArr[1].val);

            portInArr[0] = grid.Portables().Builder(typeof(CompositeInner)).SetField("val", 2).Build();

            portObj = grid.Portables().Builder(portObj).HashCode(200)
                .SetField("inArr", portInArr).Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            portInArr = portObj.Field<IPortableObject[]>("inArr");

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(2, portInArr[0].Field<int>("val"));
            Assert.AreEqual(1, portInArr[1].Field<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.outArr);
            Assert.AreEqual(2, arr.inArr.Length);
            Assert.AreEqual(2, arr.inArr[0].val);
            Assert.AreEqual(1, arr.inArr[1].val);

            // 4. Test nested object handle inversion.
            CompositeOuter[] outArr = { new CompositeOuter(inner), new CompositeOuter(inner) };

            portObj = grid.Portables().Builder(typeof(CompositeArray)).HashCode(100)
                .SetField("outArr", outArr).Build();

            meta = portObj.Metadata();

            Assert.AreEqual(typeof(CompositeArray).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_OBJECT, meta.FieldTypeName("inArr"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_ARRAY_OBJECT, meta.FieldTypeName("outArr"));

            Assert.AreEqual(100, portObj.GetHashCode());

            IPortableObject[] portOutArr = portObj.Field<IPortableObject[]>("outArr");

            Assert.AreEqual(2, portOutArr.Length);
            Assert.AreEqual(1, portOutArr[0].Field<IPortableObject>("inner").Field<int>("val"));
            Assert.AreEqual(1, portOutArr[1].Field<IPortableObject>("inner").Field<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.inArr);
            Assert.AreEqual(2, arr.outArr.Length);
            Assert.AreEqual(1, arr.outArr[0].inner.val);
            Assert.AreEqual(1, arr.outArr[1].inner.val);

            portOutArr[0] = grid.Portables().Builder(typeof(CompositeOuter))
                .SetField("inner", new CompositeInner(2)).Build();

            portObj = grid.Portables().Builder(portObj).HashCode(200)
                .SetField("outArr", portOutArr).Build();

            Assert.AreEqual(200, portObj.GetHashCode());

            portInArr = portObj.Field<IPortableObject[]>("outArr");

            Assert.AreEqual(2, portInArr.Length);
            Assert.AreEqual(2, portOutArr[0].Field<IPortableObject>("inner").Field<int>("val"));
            Assert.AreEqual(1, portOutArr[1].Field<IPortableObject>("inner").Field<int>("val"));

            arr = portObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.inArr);
            Assert.AreEqual(2, arr.outArr.Length);
            Assert.AreEqual(2, arr.outArr[0].inner.val);
            Assert.AreEqual(1, arr.outArr[1].inner.val);
        }

        /// <summary>
        /// Test container types other than array.
        /// </summary>
        [Test]
        public void TestCompositeContainer()
        {
            ArrayList col = new ArrayList();
            ICollection<CompositeInner> gCol = new List<CompositeInner>();
            IDictionary dict = new Hashtable();
            IDictionary<int, CompositeInner> gDict = new Dictionary<int, CompositeInner>();

            col.Add(new CompositeInner(1));
            gCol.Add(new CompositeInner(2));
            dict[3] = new CompositeInner(3);
            gDict[4] = new CompositeInner(4);

            IPortableObject portObj = grid.Portables().Builder(typeof(CompositeContainer)).HashCode(100)
                .SetField<ICollection>("col", col)
                .SetField("gCol", gCol)
                .SetField("dict", dict)
                .SetField("gDict", gDict).Build();

            // 1. Check meta.
            IPortableMetadata meta = portObj.Metadata();

            Assert.AreEqual(typeof(CompositeContainer).Name, meta.TypeName);

            Assert.AreEqual(4, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_COLLECTION, meta.FieldTypeName("col"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_COLLECTION, meta.FieldTypeName("gCol"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_MAP, meta.FieldTypeName("dict"));
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_MAP, meta.FieldTypeName("gDict"));

            // 2. Check in portable form.
            Assert.AreEqual(1, portObj.Field<ICollection>("col").Count);
            Assert.AreEqual(1, portObj.Field<ICollection>("col").OfType<IPortableObject>().First()
                .Field<int>("val"));

            Assert.AreEqual(1, portObj.Field<ICollection<IPortableObject>>("gCol").Count);
            Assert.AreEqual(2, portObj.Field<ICollection<IPortableObject>>("gCol").First().Field<int>("val"));

            Assert.AreEqual(1, portObj.Field<IDictionary>("dict").Count);
            Assert.AreEqual(3, ((IPortableObject) portObj.Field<IDictionary>("dict")[3]).Field<int>("val"));

            Assert.AreEqual(1, portObj.Field<IDictionary<int, IPortableObject>>("gDict").Count);
            Assert.AreEqual(4, portObj.Field<IDictionary<int, IPortableObject>>("gDict")[4].Field<int>("val"));

            // 3. Check in deserialized form.
            CompositeContainer obj = portObj.Deserialize<CompositeContainer>();

            Assert.AreEqual(1, obj.col.Count);
            Assert.AreEqual(1, obj.col.OfType<CompositeInner>().First().val);

            Assert.AreEqual(1, obj.gCol.Count);
            Assert.AreEqual(2, obj.gCol.First().val);

            Assert.AreEqual(1, obj.dict.Count);
            Assert.AreEqual(3, ((CompositeInner) obj.dict[3]).val);

            Assert.AreEqual(1, obj.gDict.Count);
            Assert.AreEqual(4, obj.gDict[4].val);
        }

        /// <summary>
        /// Ensure that raw data is not lost during build.
        /// </summary>
        [Test]
        public void TestRawData()
        {
            WithRaw raw = new WithRaw();

            raw.a = 1;
            raw.b = 2;

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(marsh.Marshal(raw), PortableMode.FORCE_PORTABLE);

            raw = portObj.Deserialize<WithRaw>();

            Assert.AreEqual(1, raw.a);
            Assert.AreEqual(2, raw.b);

            IPortableObject newPortObj = grid.Portables().Builder(portObj).SetField("a", 3).Build();

            raw = newPortObj.Deserialize<WithRaw>();

            Assert.AreEqual(3, raw.a);
            Assert.AreEqual(2, raw.b);
        }

        /// <summary>
        /// Test nested objects.
        /// </summary>
        [Test]
        public void TestNested()
        {
            // 1. Create from scratch.
            IPortableBuilder builder = grid.Portables().Builder(typeof(NestedOuter));

            NestedInner inner1 = new NestedInner();
            inner1.val = 1;
            builder.SetField("inner1", inner1);

            IPortableObject outerPortObj = builder.Build();

            IPortableMetadata meta = outerPortObj.Metadata();

            Assert.AreEqual(typeof(NestedOuter).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_OBJECT, meta.FieldTypeName("inner1"));

            IPortableObject innerPortObj1 = outerPortObj.Field<IPortableObject>("inner1");

            IPortableMetadata innerMeta = innerPortObj1.Metadata();

            Assert.AreEqual(typeof(NestedInner).Name, innerMeta.TypeName);
            Assert.AreEqual(1, innerMeta.Fields.Count);
            Assert.AreEqual(PortableTypeNames.TYPE_NAME_INT, innerMeta.FieldTypeName("val"));

            inner1 = innerPortObj1.Deserialize<NestedInner>();

            Assert.AreEqual(1, inner1.val);

            NestedOuter outer = outerPortObj.Deserialize<NestedOuter>();
            Assert.AreEqual(outer.inner1.val, 1);
            Assert.IsNull(outer.inner2);

            // 2. Add another field over existing portable object.
            builder = grid.Portables().Builder(outerPortObj);

            NestedInner inner2 = new NestedInner();
            inner2.val = 2;
            builder.SetField("inner2", inner2);

            outerPortObj = builder.Build();

            outer = outerPortObj.Deserialize<NestedOuter>();
            Assert.AreEqual(1, outer.inner1.val);
            Assert.AreEqual(2, outer.inner2.val);

            // 3. Try setting inner object in portable form.
            innerPortObj1 = grid.Portables().Builder(innerPortObj1).SetField("val", 3).Build();

            inner1 = innerPortObj1.Deserialize<NestedInner>();

            Assert.AreEqual(3, inner1.val);

            outerPortObj = grid.Portables().Builder(outerPortObj).SetField<object>("inner1", innerPortObj1).Build();

            outer = outerPortObj.Deserialize<NestedOuter>();
            Assert.AreEqual(3, outer.inner1.val);
            Assert.AreEqual(2, outer.inner2.val);
        }

        /// <summary>
        /// Test handle migration.
        /// </summary>
        [Test]
        public void TestHandleMigration()
        {
            // 1. Simple comparison of results.
            MigrationInner inner = new MigrationInner();
            inner.val = 1;

            MigrationOuter outer = new MigrationOuter();
            outer.inner1 = inner;
            outer.inner2 = inner;

            byte[] outerBytes = marsh.Marshal(outer);

            IPortableBuilder builder = grid.Portables().Builder(typeof(MigrationOuter));

            builder.HashCode(outer.GetHashCode());

            builder.SetField<object>("inner1", inner);
            builder.SetField<object>("inner2", inner);

            PortableUserObject portOuter = (PortableUserObject) builder.Build();

            byte[] portOuterBytes = new byte[outerBytes.Length];

            Buffer.BlockCopy(portOuter.Data, 0, portOuterBytes, 0, portOuterBytes.Length);

            Assert.AreEqual(outerBytes, portOuterBytes);

            // 2. Change the first inner object so that the handle must migrate.
            MigrationInner inner1 = new MigrationInner();
            inner1.val = 2;

            IPortableObject portOuterMigrated =
                grid.Portables().Builder(portOuter).SetField<object>("inner1", inner1).Build();

            MigrationOuter outerMigrated = portOuterMigrated.Deserialize<MigrationOuter>();

            Assert.AreEqual(2, outerMigrated.inner1.val);
            Assert.AreEqual(1, outerMigrated.inner2.val);

            // 3. Change the first value using serialized form.
            IPortableObject inner1Port =
                grid.Portables().Builder(typeof(MigrationInner)).SetField("val", 2).Build();

            portOuterMigrated =
                grid.Portables().Builder(portOuter).SetField<object>("inner1", inner1Port).Build();

            outerMigrated = portOuterMigrated.Deserialize<MigrationOuter>();

            Assert.AreEqual(2, outerMigrated.inner1.val);
            Assert.AreEqual(1, outerMigrated.inner2.val);
        }

        /// <summary>
        /// Test handle inversion.
        /// </summary>
        [Test]
        public void TestHandleInversion()
        {
            InversionInner inner = new InversionInner();
            InversionOuter outer = new InversionOuter();

            inner.outer = outer;
            outer.inner = inner;

            byte[] rawOuter = marsh.Marshal(outer);

            IPortableObject portOuter = marsh.Unmarshal<IPortableObject>(rawOuter, PortableMode.FORCE_PORTABLE);
            IPortableObject portInner = portOuter.Field<IPortableObject>("inner");

            // 1. Ensure that inner object can be deserialized after build.
            IPortableObject portInnerNew = grid.Portables().Builder(portInner).Build();

            InversionInner innerNew = portInnerNew.Deserialize<InversionInner>();

            Assert.AreSame(innerNew, innerNew.outer.inner);

            // 2. Ensure that portable object with external dependencies could be added to builder.
            IPortableObject portOuterNew =
                grid.Portables().Builder(typeof(InversionOuter)).SetField<object>("inner", portInner).Build();

            InversionOuter outerNew = portOuterNew.Deserialize<InversionOuter>();

            Assert.AreNotSame(outerNew, outerNew.inner.outer);
            Assert.AreSame(outerNew.inner, outerNew.inner.outer.inner);
        }

        /// <summary>
        /// Test build multiple objects.
        /// </summary>
        [Test]
        public void TestBuildMultiple()
        {
            IPortableBuilder builder = grid.Portables().Builder(typeof(Primitives));

            builder.SetField<byte>("fByte", 1).SetField("fBool", true);

            IPortableObject po1 = builder.Build();
            IPortableObject po2 = builder.Build();

            Assert.AreEqual(1, po1.Field<byte>("fByte"));
            Assert.AreEqual(true, po1.Field<bool>("fBool"));

            Assert.AreEqual(1, po2.Field<byte>("fByte"));
            Assert.AreEqual(true, po2.Field<bool>("fBool"));

            builder.SetField<byte>("fByte", 2);

            IPortableObject po3 = builder.Build();

            Assert.AreEqual(1, po1.Field<byte>("fByte"));
            Assert.AreEqual(true, po1.Field<bool>("fBool"));

            Assert.AreEqual(1, po2.Field<byte>("fByte"));
            Assert.AreEqual(true, po2.Field<bool>("fBool"));

            Assert.AreEqual(2, po3.Field<byte>("fByte"));
            Assert.AreEqual(true, po2.Field<bool>("fBool"));

            builder = grid.Portables().Builder(po1);

            builder.SetField<byte>("fByte", 10);

            po1 = builder.Build();
            po2 = builder.Build();

            builder.SetField<byte>("fByte", 20);

            po3 = builder.Build();

            Assert.AreEqual(10, po1.Field<byte>("fByte"));
            Assert.AreEqual(true, po1.Field<bool>("fBool"));

            Assert.AreEqual(10, po2.Field<byte>("fByte"));
            Assert.AreEqual(true, po2.Field<bool>("fBool"));

            Assert.AreEqual(20, po3.Field<byte>("fByte"));
            Assert.AreEqual(true, po3.Field<bool>("fBool"));
        }

        /// <summary>
        /// Tests type id method.
        /// </summary>
        [Test]
        public void TestTypeId()
        {
            Assert.Throws<ArgumentException>(() => grid.Portables().GetTypeId(null));

            Assert.AreEqual(IdMapper.TEST_TYPE_ID, grid.Portables().GetTypeId(IdMapper.TEST_TYPE_NAME));
            
            Assert.AreEqual(PortableUtils.StringHashCode("someTypeName"), grid.Portables().GetTypeId("someTypeName"));
        }

        /// <summary>
        /// Tests metadata methods.
        /// </summary>
        [Test]
        public void TestMetadata()
        {
            // Populate metadata
            var portables = grid.Portables();

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
            PortableTypeConfiguration cfg = new PortableTypeConfiguration(typ);

            cfg.MetadataEnabled = false;

            return cfg;
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
        public byte fByte;
        public bool fBool;
        public short fShort;
        public char fChar;
        public int fInt;
        public long fLong;
        public float fFloat;
        public double fDouble;
    }

    /// <summary>
    /// Portable with primitive array fields.
    /// </summary>
    public class PrimitiveArrays
    {
        public byte[] fByte;
        public bool[] fBool;
        public short[] fShort;
        public char[] fChar;
        public int[] fInt;
        public long[] fLong;
        public float[] fFloat;
        public double[] fDouble;
    }

    /// <summary>
    /// Portable having strings, dates, Guids and enums.
    /// </summary>
    public class StringDateGuidEnum
    {
        public string fStr;
        public DateTime fDate;
        public DateTime? fNDate;
        public Guid fGuid;
        public Guid? fNGuid;
        public TestEnum fEnum;

        public string[] fStrArr;
        public DateTime?[] fDateArr;
        public Guid?[] fGuidArr;
        public TestEnum[] fEnumArr;
    }

    /// <summary>
    /// Enumeration.
    /// </summary>
    public enum TestEnum
    {
        ONE, TWO
    }

    /// <summary>
    /// Portable with raw data.
    /// </summary>
    public class WithRaw : IPortableMarshalAware
    {
        public int a;
        public int b;

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteInt("a", a);
            writer.RawWriter().WriteInt(b);
        }

        /** <inheritDoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            a = reader.ReadInt("a");
            b = reader.RawReader().ReadInt();
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
        public NestedInner inner1;
        public NestedInner inner2;
    }

    /// <summary>
    /// Nested inner object.
    /// </summary>
    public class NestedInner
    {
        public int val;
    }

    /// <summary>
    /// Outer object for handle migration test.
    /// </summary>
    public class MigrationOuter
    {
        public MigrationInner inner1;
        public MigrationInner inner2;
    }

    /// <summary>
    /// Inner object for handle migration test.
    /// </summary>
    public class MigrationInner
    {
        public int val;
    }

    /// <summary>
    /// Outer object for handle inversion test.
    /// </summary>
    public class InversionOuter
    {
        public InversionInner inner;
    }

    /// <summary>
    /// Inner object for handle inversion test.
    /// </summary>
    public class InversionInner
    {
        public InversionOuter outer;
    }

    /// <summary>
    /// Object for composite array tests.
    /// </summary>
    public class CompositeArray
    {
        public CompositeInner[] inArr;
        public CompositeOuter[] outArr;
    }

    /// <summary>
    /// Object for composite collection/dictionary tests.
    /// </summary>
    public class CompositeContainer
    {
        public ICollection col;
        public ICollection<CompositeInner> gCol;

        public IDictionary dict;
        public IDictionary<int, CompositeInner> gDict;
    }

    /// <summary>
    /// OUter object for composite structures test.
    /// </summary>
    public class CompositeOuter
    {
        public CompositeInner inner;

        public CompositeOuter()
        {
            // No-op.
        }

        public CompositeOuter(CompositeInner inner)
        {
            this.inner = inner;
        }
    }

    /// <summary>
    /// Inner object for composite structures test.
    /// </summary>
    public class CompositeInner
    {
        public int val;

        public CompositeInner()
        {
            // No-op.
        }

        public CompositeInner(int val)
        {
            this.val = val;
        }
    }

    /// <summary>
    /// Type to test "ToPortable()" logic.
    /// </summary>
    public class ToPortable
    {
        public int val;

        public ToPortable(int val)
        {
            this.val = val;
        }
    }

    /// <summary>
    /// Type to test "ToPortable()" logic with metadata disabled.
    /// </summary>
    public class ToPortableNoMeta
    {
        public int val;

        public ToPortableNoMeta(int val)
        {
            this.val = val;
        }
    }

    /// <summary>
    /// Type to test removal.
    /// </summary>
    public class Remove
    {
        public object val;
        public RemoveInner val2;
    }

    /// <summary>
    /// Inner type to test removal.
    /// </summary>
    public class RemoveInner
    {
        /** */
        public int val;

        /// <summary>
        ///
        /// </summary>
        /// <param name="val"></param>
        public RemoveInner(int val)
        {
            this.val = val;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderInBuilderOuter
    {
        /** */
        public BuilderInBuilderInner inner;

        /** */
        public BuilderInBuilderInner inner2;
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderInBuilderInner
    {
        /** */
        public BuilderInBuilderOuter outer;
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderCollection
    {
        /** */
        public ICollection<BuilderCollectionItem> col;

        /// <summary>
        ///
        /// </summary>
        /// <param name="col"></param>
        public BuilderCollection(ICollection<BuilderCollectionItem> col)
        {
            this.col = col;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class BuilderCollectionItem
    {
        /** */
        public int val;

        /// <summary>
        ///
        /// </summary>
        /// <param name="val"></param>
        public BuilderCollectionItem(int val)
        {
            this.val = val;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class DecimalHolder
    {
        /** */
        public decimal val;

        /** */
        public decimal[] valArr;
    }

    /// <summary>
    /// Test id mapper.
    /// </summary>
    public class IdMapper : IPortableIdMapper
    {
        /** */
        public const string TEST_TYPE_NAME = "IdMapperTestType";

        /** */
        public const int TEST_TYPE_ID = -65537;

        /** <inheritdoc /> */
        public int TypeId(string typeName)
        {
            return typeName == TEST_TYPE_NAME ? TEST_TYPE_ID : 0;
        }

        /** <inheritdoc /> */
        public int FieldId(int typeId, string fieldName)
        {
            return 0;
        }
    }
}
