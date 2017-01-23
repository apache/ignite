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
// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Binary builder self test.
    /// </summary>
    public class BinaryBuilderSelfTest
    {
        /** Undefined type: Empty. */
        private const string TypeEmpty = "EmptyUndefined";

        /** Grid. */
        private Ignite _grid;

        /** Marshaller. */
        private Marshaller _marsh;

        /// <summary>
        /// Set up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof(Empty)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(Primitives)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(PrimitiveArrays)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(StringDateGuidEnum))
                        {
                            EqualityComparer = GetIdentityResolver()
                        },
                        new BinaryTypeConfiguration(typeof(WithRaw)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(MetaOverwrite)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(NestedOuter)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(NestedInner)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(MigrationOuter)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(MigrationInner)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(InversionOuter)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(InversionInner)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(CompositeOuter)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(CompositeInner)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(CompositeArray)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(CompositeContainer))
                        {
                            EqualityComparer = GetIdentityResolver()
                        },
                        new BinaryTypeConfiguration(typeof(ToBinary)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(Remove)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(RemoveInner)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(BuilderInBuilderOuter))
                        {
                            EqualityComparer = GetIdentityResolver()
                        },
                        new BinaryTypeConfiguration(typeof(BuilderInBuilderInner))
                        {
                            EqualityComparer = GetIdentityResolver()
                        },
                        new BinaryTypeConfiguration(typeof(BuilderCollection))
                        {
                            EqualityComparer = GetIdentityResolver()
                        },
                        new BinaryTypeConfiguration(typeof(BuilderCollectionItem))
                        {
                            EqualityComparer = GetIdentityResolver()
                        },
                        new BinaryTypeConfiguration(typeof(DecimalHolder)) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(TypeEmpty) {EqualityComparer = GetIdentityResolver()},
                        new BinaryTypeConfiguration(typeof(TestEnumRegistered))
                        {
                            EqualityComparer = GetIdentityResolver()
                        },
                        new BinaryTypeConfiguration(typeof(NameMapperTestType))
                        {
                            EqualityComparer = GetIdentityResolver()
                        }
                    },
                    DefaultIdMapper = new IdMapper(),
                    DefaultNameMapper = new NameMapper(),
                    CompactFooter = GetCompactFooter()
                }
            };

            _grid = (Ignite) Ignition.Start(cfg);

            _marsh = _grid.Marshaller;
        }

        /// <summary>
        /// Gets the compact footer setting.
        /// </summary>
        protected virtual bool GetCompactFooter()
        {
            return true;
        }

        /// <summary>
        /// Gets the identity resolver.
        /// </summary>
        protected virtual IEqualityComparer<IBinaryObject> GetIdentityResolver()
        {
            return null;
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TearDown()
        {
            if (_grid != null)
                Ignition.Stop(_grid.Name, true);

            _grid = null;
        }

        /// <summary>
        /// Ensure that binary engine is able to work with type names, which are not configured.
        /// </summary>
        [Test]
        public void TestNonConfigured()
        {
            string typeName1 = "Type1";
            string typeName2 = "Type2";
            string field1 = "field1";
            string field2 = "field2";

            // 1. Ensure that builder works fine.
            IBinaryObject binObj1 = _grid.GetBinary().GetBuilder(typeName1).SetField(field1, 1).Build();

            Assert.AreEqual(typeName1, binObj1.GetBinaryType().TypeName);
            Assert.AreEqual(1, binObj1.GetBinaryType().Fields.Count);
            Assert.AreEqual(field1, binObj1.GetBinaryType().Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binObj1.GetBinaryType().GetFieldTypeName(field1));

            Assert.AreEqual(1, binObj1.GetField<int>(field1));

            // 2. Ensure that object can be unmarshalled without deserialization.
            byte[] data = ((BinaryObject) binObj1).Data;

            binObj1 = _grid.Marshaller.Unmarshal<IBinaryObject>(data, BinaryMode.ForceBinary);

            Assert.AreEqual(typeName1, binObj1.GetBinaryType().TypeName);
            Assert.AreEqual(1, binObj1.GetBinaryType().Fields.Count);
            Assert.AreEqual(field1, binObj1.GetBinaryType().Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binObj1.GetBinaryType().GetFieldTypeName(field1));

            Assert.AreEqual(1, binObj1.GetField<int>(field1));

            // 3. Ensure that we can nest one anonymous object inside another
            IBinaryObject binObj2 =
                _grid.GetBinary().GetBuilder(typeName2).SetField(field2, binObj1).Build();

            Assert.AreEqual(typeName2, binObj2.GetBinaryType().TypeName);
            Assert.AreEqual(1, binObj2.GetBinaryType().Fields.Count);
            Assert.AreEqual(field2, binObj2.GetBinaryType().Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, binObj2.GetBinaryType().GetFieldTypeName(field2));

            binObj1 = binObj2.GetField<IBinaryObject>(field2);

            Assert.AreEqual(typeName1, binObj1.GetBinaryType().TypeName);
            Assert.AreEqual(1, binObj1.GetBinaryType().Fields.Count);
            Assert.AreEqual(field1, binObj1.GetBinaryType().Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binObj1.GetBinaryType().GetFieldTypeName(field1));

            Assert.AreEqual(1, binObj1.GetField<int>(field1));

            // 4. Ensure that we can unmarshal object with other nested object.
            data = ((BinaryObject) binObj2).Data;

            binObj2 = _grid.Marshaller.Unmarshal<IBinaryObject>(data, BinaryMode.ForceBinary);

            Assert.AreEqual(typeName2, binObj2.GetBinaryType().TypeName);
            Assert.AreEqual(1, binObj2.GetBinaryType().Fields.Count);
            Assert.AreEqual(field2, binObj2.GetBinaryType().Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, binObj2.GetBinaryType().GetFieldTypeName(field2));

            binObj1 = binObj2.GetField<IBinaryObject>(field2);

            Assert.AreEqual(typeName1, binObj1.GetBinaryType().TypeName);
            Assert.AreEqual(1, binObj1.GetBinaryType().Fields.Count);
            Assert.AreEqual(field1, binObj1.GetBinaryType().Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binObj1.GetBinaryType().GetFieldTypeName(field1));

            Assert.AreEqual(1, binObj1.GetField<int>(field1));
        }

        /// <summary>
        /// Test "ToBinary()" method.
        /// </summary>
        [Test]
        public void TestToBinary()
        {
            DateTime date = DateTime.Now.ToUniversalTime();
            Guid guid = Guid.NewGuid();

            IBinary api = _grid.GetBinary();

            // 1. Primitives.
            Assert.AreEqual(1, api.ToBinary<byte>((byte)1));
            Assert.AreEqual(1, api.ToBinary<short>((short)1));
            Assert.AreEqual(1, api.ToBinary<int>(1));
            Assert.AreEqual(1, api.ToBinary<long>((long)1));

            Assert.AreEqual((float)1, api.ToBinary<float>((float)1));
            Assert.AreEqual((double)1, api.ToBinary<double>((double)1));

            Assert.AreEqual(true, api.ToBinary<bool>(true));
            Assert.AreEqual('a', api.ToBinary<char>('a'));

            // 2. Special types.
            Assert.AreEqual("a", api.ToBinary<string>("a"));
            Assert.AreEqual(date, api.ToBinary<DateTime>(date));
            Assert.AreEqual(guid, api.ToBinary<Guid>(guid));
            Assert.AreEqual(TestEnumRegistered.One, api.ToBinary<IBinaryObject>(TestEnumRegistered.One)
                .Deserialize<TestEnumRegistered>());

            // 3. Arrays.
            Assert.AreEqual(new byte[] { 1 }, api.ToBinary<byte[]>(new byte[] { 1 }));
            Assert.AreEqual(new short[] { 1 }, api.ToBinary<short[]>(new short[] { 1 }));
            Assert.AreEqual(new[] { 1 }, api.ToBinary<int[]>(new[] { 1 }));
            Assert.AreEqual(new long[] { 1 }, api.ToBinary<long[]>(new long[] { 1 }));

            Assert.AreEqual(new float[] { 1 }, api.ToBinary<float[]>(new float[] { 1 }));
            Assert.AreEqual(new double[] { 1 }, api.ToBinary<double[]>(new double[] { 1 }));

            Assert.AreEqual(new[] { true }, api.ToBinary<bool[]>(new[] { true }));
            Assert.AreEqual(new[] { 'a' }, api.ToBinary<char[]>(new[] { 'a' }));

            Assert.AreEqual(new[] { "a" }, api.ToBinary<string[]>(new[] { "a" }));
            Assert.AreEqual(new[] { date }, api.ToBinary<DateTime[]>(new[] { date }));
            Assert.AreEqual(new[] { guid }, api.ToBinary<Guid[]>(new[] { guid }));
            Assert.AreEqual(new[] { TestEnumRegistered.One},
                api.ToBinary<IBinaryObject[]>(new[] { TestEnumRegistered.One})
                .Select(x => x.Deserialize<TestEnumRegistered>()).ToArray());

            // 4. Objects.
            IBinaryObject binObj = api.ToBinary<IBinaryObject>(new ToBinary(1));

            Assert.AreEqual(typeof(ToBinary).Name, binObj.GetBinaryType().TypeName);
            Assert.AreEqual(1, binObj.GetBinaryType().Fields.Count);
            Assert.AreEqual("Val", binObj.GetBinaryType().Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binObj.GetBinaryType().GetFieldTypeName("Val"));

            Assert.AreEqual(1, binObj.GetField<int>("val"));
            Assert.AreEqual(1, binObj.Deserialize<ToBinary>().Val);

            // 5. Object array.
            var binObjArr = api.ToBinary<object[]>(new object[] {new ToBinary(1)})
                .OfType<IBinaryObject>().ToArray();

            Assert.AreEqual(1, binObjArr.Length);
            Assert.AreEqual(1, binObjArr[0].GetField<int>("Val"));
            Assert.AreEqual(1, binObjArr[0].Deserialize<ToBinary>().Val);
        }

        /// <summary>
        /// Test builder field remove logic.
        /// </summary>
        [Test]
        public void TestRemove()
        {
            // Create empty object.
            IBinaryObject binObj = _grid.GetBinary().GetBuilder(typeof(Remove)).Build();

            Assert.IsNull(binObj.GetField<object>("val"));
            Assert.IsNull(binObj.Deserialize<Remove>().Val);

            IBinaryType meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(Remove).Name, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);

            // Populate it with field.
            IBinaryObjectBuilder builder = binObj.ToBuilder();

            Assert.IsNull(builder.GetField<object>("val"));

            object val = 1;

            builder.SetField("val", val);

            Assert.AreEqual(val, builder.GetField<object>("val"));

            binObj = builder.Build();

            Assert.AreEqual(val, binObj.GetField<object>("val"));
            Assert.AreEqual(val, binObj.Deserialize<Remove>().Val);

            meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(Remove).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("val", meta.Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, meta.GetFieldTypeName("val"));

            // Perform field remove.
            builder = binObj.ToBuilder();

            Assert.AreEqual(val, builder.GetField<object>("val"));

            builder.RemoveField("val");
            Assert.IsNull(builder.GetField<object>("val"));

            builder.SetField("val", val);
            Assert.AreEqual(val, builder.GetField<object>("val"));

            builder.RemoveField("val");
            Assert.IsNull(builder.GetField<object>("val"));

            binObj = builder.Build();

            Assert.IsNull(binObj.GetField<object>("val"));
            Assert.IsNull(binObj.Deserialize<Remove>().Val);

            // Test correct removal of field being referenced by handle somewhere else.
            RemoveInner inner = new RemoveInner(2);

            binObj = _grid.GetBinary().GetBuilder(typeof(Remove))
                .SetField("val", inner)
                .SetField("val2", inner)
                .Build();

            binObj = binObj.ToBuilder().RemoveField("val").Build();

            Remove obj = binObj.Deserialize<Remove>();

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
            IBinaryObjectBuilder builderOuter = _grid.GetBinary().GetBuilder(typeof(BuilderInBuilderOuter));
            IBinaryObjectBuilder builderInner = _grid.GetBinary().GetBuilder(typeof(BuilderInBuilderInner));

            builderOuter.SetField<object>("inner", builderInner);
            builderInner.SetField<object>("outer", builderOuter);

            IBinaryObject outerbinObj = builderOuter.Build();

            IBinaryType meta = outerbinObj.GetBinaryType();

            Assert.AreEqual(typeof(BuilderInBuilderOuter).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("inner", meta.Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, meta.GetFieldTypeName("inner"));

            IBinaryObject innerbinObj = outerbinObj.GetField<IBinaryObject>("inner");

            meta = innerbinObj.GetBinaryType();

            Assert.AreEqual(typeof(BuilderInBuilderInner).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("outer", meta.Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, meta.GetFieldTypeName("outer"));

            BuilderInBuilderOuter outer = outerbinObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer, outer.Inner.Outer);

            // Test same builders assembly.
            innerbinObj = _grid.GetBinary().GetBuilder(typeof(BuilderInBuilderInner)).Build();

            outerbinObj = _grid.GetBinary().GetBuilder(typeof(BuilderInBuilderOuter))
                .SetField("inner", innerbinObj)
                .SetField("inner2", innerbinObj)
                .Build();

            meta = outerbinObj.GetBinaryType();

            Assert.AreEqual(typeof(BuilderInBuilderOuter).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.IsTrue(meta.Fields.Contains("inner"));
            Assert.IsTrue(meta.Fields.Contains("inner2"));
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, meta.GetFieldTypeName("inner"));
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, meta.GetFieldTypeName("inner2"));

            outer = outerbinObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer.Inner, outer.Inner2);

            builderOuter = _grid.GetBinary().GetBuilder(outerbinObj);
            IBinaryObjectBuilder builderInner2 = builderOuter.GetField<IBinaryObjectBuilder>("inner2");

            builderInner2.SetField("outer", builderOuter);

            outerbinObj = builderOuter.Build();

            outer = outerbinObj.Deserialize<BuilderInBuilderOuter>();

            Assert.AreSame(outer, outer.Inner.Outer);
            Assert.AreSame(outer.Inner, outer.Inner2);
        }

        /// <summary>
        /// Test for decimals building.
        /// </summary>
        [Test]
        public void TestDecimals()
        {
            IBinaryObject binObj = _grid.GetBinary().GetBuilder(typeof(DecimalHolder))
                .SetField("val", decimal.One)
                .SetField("valArr", new decimal?[] { decimal.MinusOne })
                .Build();

            IBinaryType meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(DecimalHolder).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.IsTrue(meta.Fields.Contains("val"));
            Assert.IsTrue(meta.Fields.Contains("valArr"));
            Assert.AreEqual(BinaryTypeNames.TypeNameDecimal, meta.GetFieldTypeName("val"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayDecimal, meta.GetFieldTypeName("valArr"));

            Assert.AreEqual(decimal.One, binObj.GetField<decimal>("val"));
            Assert.AreEqual(new decimal?[] { decimal.MinusOne }, binObj.GetField<decimal?[]>("valArr"));

            DecimalHolder obj = binObj.Deserialize<DecimalHolder>();

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
            IBinaryObjectBuilder builderCol = _grid.GetBinary().GetBuilder(typeof(BuilderCollection));
            IBinaryObjectBuilder builderItem =
                _grid.GetBinary().GetBuilder(typeof(BuilderCollectionItem)).SetField("val", 1);

            builderCol.SetCollectionField("col", new ArrayList { builderItem });

            IBinaryObject binCol = builderCol.Build();

            IBinaryType meta = binCol.GetBinaryType();

            Assert.AreEqual(typeof(BuilderCollection).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("col", meta.Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameCollection, meta.GetFieldTypeName("col"));

            var binColItems = binCol.GetField<ArrayList>("col");

            Assert.AreEqual(1, binColItems.Count);

            var binItem = (IBinaryObject) binColItems[0];

            meta = binItem.GetBinaryType();

            Assert.AreEqual(typeof(BuilderCollectionItem).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual("val", meta.Fields.First());
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, meta.GetFieldTypeName("val"));

            BuilderCollection col = binCol.Deserialize<BuilderCollection>();

            Assert.IsNotNull(col.Col);
            Assert.AreEqual(1, col.Col.Count);
            Assert.AreEqual(1, ((BuilderCollectionItem) col.Col[0]).Val);

            // Add more binary objects to collection.
            builderCol = _grid.GetBinary().GetBuilder(binCol);

            IList builderColItems = builderCol.GetField<IList>("col");

            Assert.AreEqual(1, builderColItems.Count);

            BinaryObjectBuilder builderColItem = (BinaryObjectBuilder) builderColItems[0];

            builderColItem.SetField("val", 2); // Change nested value.

            builderColItems.Add(builderColItem); // Add the same object to check handles.
            builderColItems.Add(builderItem); // Add item from another builder.
            builderColItems.Add(binItem); // Add item in binary form.

            binCol = builderCol.Build();

            col = binCol.Deserialize<BuilderCollection>();

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
            builderCol = _grid.GetBinary().GetBuilder(binCol);

            builderColItems = builderCol.GetField<IList>("col");

            ((BinaryObjectBuilder) builderColItems[1]).SetField("val", 3);

            binCol = builderCol.Build();

            col = binCol.Deserialize<BuilderCollection>();

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
            IBinaryObject binObj = _grid.GetBinary().GetBuilder(typeof(Empty)).Build();

            Assert.IsNotNull(binObj);
            Assert.AreEqual(GetIdentityResolver() == null ? 0 : 1, binObj.GetHashCode());

            IBinaryType meta = binObj.GetBinaryType();

            Assert.IsNotNull(meta);
            Assert.AreEqual(typeof(Empty).Name, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);

            Empty obj = binObj.Deserialize<Empty>();

            Assert.IsNotNull(obj);
        }

        /// <summary>
        /// Test build of an empty undefined object.
        /// </summary>
        [Test]
        public void TestEmptyUndefined()
        {
            IBinaryObject binObj = _grid.GetBinary().GetBuilder(TypeEmpty).Build();

            Assert.IsNotNull(binObj);
            Assert.AreEqual(GetIdentityResolver() == null ? 0 : 1, binObj.GetHashCode());

            IBinaryType meta = binObj.GetBinaryType();

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
            var binObj = (BinaryObject) _grid.GetBinary().GetBuilder(typeof(Empty)).Build();

            BinaryObject newbinObj = (BinaryObject) _grid.GetBinary().GetBuilder(binObj).Build();

            Assert.AreEqual(binObj.Data, newbinObj.Data);
        }

        /// <summary>
        /// Test hash code alteration.
        /// </summary>
        [Test]
        public void TestHashCodeChange()
        {
            IBinaryObject binObj = _grid.GetBinary().GetBuilder(typeof(Empty)).SetHashCode(100).Build();

            Assert.AreEqual(100, binObj.GetHashCode());
        }

        /// <summary>
        /// Tests equality and formatting members.
        /// </summary>
        [Test]
        public void TestEquality()
        {
            var bin = _grid.GetBinary();

            var obj1 = bin.GetBuilder("myType").SetStringField("str", "foo").SetIntField("int", 1).Build();
            var obj2 = bin.GetBuilder("myType").SetStringField("str", "foo").SetIntField("int", 1).Build();

            Assert.AreEqual(obj1, obj2);

            Assert.AreEqual(0, obj1.GetHashCode());
            Assert.AreEqual(0, obj2.GetHashCode());

            Assert.IsTrue(Regex.IsMatch(obj1.ToString(), @"myType \[idHash=[0-9]+, str=foo, int=1\]"));
        }

        /// <summary>
        /// Test primitive fields setting.
        /// </summary>
        [Test]
        public void TestPrimitiveFields()
        {
            // Generic SetField method.
            var binObj = _grid.GetBinary().GetBuilder(typeof(Primitives))
                .SetField<byte>("fByte", 1)
                .SetField("fBool", true)
                .SetField<short>("fShort", 2)
                .SetField("fChar", 'a')
                .SetField("fInt", 3)
                .SetField<long>("fLong", 4)
                .SetField<float>("fFloat", 5)
                .SetField<double>("fDouble", 6)
                .SetField("fDecimal", 7.7m)
                .SetHashCode(100)
                .Build();

            CheckPrimitiveFields1(binObj);

            // Specific setter methods.
            var binObj2 = _grid.GetBinary().GetBuilder(typeof(Primitives))
                .SetByteField("fByte", 1)
                .SetBooleanField("fBool", true)
                .SetShortField("fShort", 2)
                .SetCharField("fChar", 'a')
                .SetIntField("fInt", 3)
                .SetLongField("fLong", 4)
                .SetFloatField("fFloat", 5)
                .SetDoubleField("fDouble", 6)
                .SetDecimalField("fDecimal", 7.7m)
                .SetHashCode(100)
                .Build();

            CheckPrimitiveFields1(binObj2);

            // Check equality.
            Assert.AreEqual(binObj, binObj2);
            Assert.AreEqual(binObj.GetHashCode(), binObj2.GetHashCode());

            // Overwrite with generic methods.
            binObj = binObj.ToBuilder()
                .SetField<byte>("fByte", 7)
                .SetField("fBool", false)
                .SetField<short>("fShort", 8)
                .SetField("fChar", 'b')
                .SetField("fInt", 9)
                .SetField<long>("fLong", 10)
                .SetField<float>("fFloat", 11)
                .SetField<double>("fDouble", 12)
                .SetField("fDecimal", 13.13m)
                .Build();

            CheckPrimitiveFields2(binObj);

            // Overwrite with specific methods.
            binObj2 = binObj.ToBuilder()
                .SetByteField("fByte", 7)
                .SetBooleanField("fBool", false)
                .SetShortField("fShort", 8)
                .SetCharField("fChar", 'b')
                .SetIntField("fInt", 9)
                .SetLongField("fLong", 10)
                .SetFloatField("fFloat", 11)
                .SetDoubleField("fDouble", 12)
                .SetDecimalField("fDecimal", 13.13m)
                .Build();

            CheckPrimitiveFields2(binObj);

            // Check equality.
            Assert.AreEqual(binObj, binObj2);
            Assert.AreEqual(binObj.GetHashCode(), binObj2.GetHashCode());
        }

        /// <summary>
        /// Checks the primitive fields values.
        /// </summary>
        private static void CheckPrimitiveFields1(IBinaryObject binObj)
        {
            Assert.AreEqual(100, binObj.GetHashCode());

            IBinaryType meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(Primitives).Name, meta.TypeName);

            Assert.AreEqual(9, meta.Fields.Count);

            Assert.AreEqual(BinaryTypeNames.TypeNameByte, meta.GetFieldTypeName("fByte"));
            Assert.AreEqual(BinaryTypeNames.TypeNameBool, meta.GetFieldTypeName("fBool"));
            Assert.AreEqual(BinaryTypeNames.TypeNameShort, meta.GetFieldTypeName("fShort"));
            Assert.AreEqual(BinaryTypeNames.TypeNameChar, meta.GetFieldTypeName("fChar"));
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, meta.GetFieldTypeName("fInt"));
            Assert.AreEqual(BinaryTypeNames.TypeNameLong, meta.GetFieldTypeName("fLong"));
            Assert.AreEqual(BinaryTypeNames.TypeNameFloat, meta.GetFieldTypeName("fFloat"));
            Assert.AreEqual(BinaryTypeNames.TypeNameDouble, meta.GetFieldTypeName("fDouble"));
            Assert.AreEqual(BinaryTypeNames.TypeNameDecimal, meta.GetFieldTypeName("fDecimal"));

            Assert.AreEqual(1, binObj.GetField<byte>("fByte"));
            Assert.AreEqual(true, binObj.GetField<bool>("fBool"));
            Assert.AreEqual(2, binObj.GetField<short>("fShort"));
            Assert.AreEqual('a', binObj.GetField<char>("fChar"));
            Assert.AreEqual(3, binObj.GetField<int>("fInt"));
            Assert.AreEqual(4, binObj.GetField<long>("fLong"));
            Assert.AreEqual(5, binObj.GetField<float>("fFloat"));
            Assert.AreEqual(6, binObj.GetField<double>("fDouble"));
            Assert.AreEqual(7.7m, binObj.GetField<decimal>("fDecimal"));

            Primitives obj = binObj.Deserialize<Primitives>();

            Assert.AreEqual(1, obj.FByte);
            Assert.AreEqual(true, obj.FBool);
            Assert.AreEqual(2, obj.FShort);
            Assert.AreEqual('a', obj.FChar);
            Assert.AreEqual(3, obj.FInt);
            Assert.AreEqual(4, obj.FLong);
            Assert.AreEqual(5, obj.FFloat);
            Assert.AreEqual(6, obj.FDouble);
            Assert.AreEqual(7.7m, obj.FDecimal);
        }

        /// <summary>
        /// Checks the primitive fields values.
        /// </summary>
        private static void CheckPrimitiveFields2(IBinaryObject binObj)
        {
            Assert.AreEqual(7, binObj.GetField<byte>("fByte"));
            Assert.AreEqual(false, binObj.GetField<bool>("fBool"));
            Assert.AreEqual(8, binObj.GetField<short>("fShort"));
            Assert.AreEqual('b', binObj.GetField<char>("fChar"));
            Assert.AreEqual(9, binObj.GetField<int>("fInt"));
            Assert.AreEqual(10, binObj.GetField<long>("fLong"));
            Assert.AreEqual(11, binObj.GetField<float>("fFloat"));
            Assert.AreEqual(12, binObj.GetField<double>("fDouble"));
            Assert.AreEqual(13.13m, binObj.GetField<decimal>("fDecimal"));

            var obj = binObj.Deserialize<Primitives>();

            Assert.AreEqual(7, obj.FByte);
            Assert.AreEqual(false, obj.FBool);
            Assert.AreEqual(8, obj.FShort);
            Assert.AreEqual('b', obj.FChar);
            Assert.AreEqual(9, obj.FInt);
            Assert.AreEqual(10, obj.FLong);
            Assert.AreEqual(11, obj.FFloat);
            Assert.AreEqual(12, obj.FDouble);
            Assert.AreEqual(13.13m, obj.FDecimal);
        }

        /// <summary>
        /// Test primitive array fields setting.
        /// </summary>
        [Test]
        public void TestPrimitiveArrayFields()
        {
            // Generic SetField method.
            var binObj = _grid.GetBinary().GetBuilder(typeof(PrimitiveArrays))
                .SetField("fByte", new byte[] { 1 })
                .SetField("fBool", new[] { true })
                .SetField("fShort", new short[] { 2 })
                .SetField("fChar", new[] { 'a' })
                .SetField("fInt", new[] { 3 })
                .SetField("fLong", new long[] { 4 })
                .SetField("fFloat", new float[] { 5 })
                .SetField("fDouble", new double[] { 6 })
                .SetField("fDecimal", new decimal?[] { 7.7m })
                .SetHashCode(100)
                .Build();

            CheckPrimitiveArrayFields1(binObj);

            // Specific setters.
            var binObj2 = _grid.GetBinary().GetBuilder(typeof(PrimitiveArrays))
                .SetByteArrayField("fByte", new byte[] {1})
                .SetBooleanArrayField("fBool", new[] {true})
                .SetShortArrayField("fShort", new short[] {2})
                .SetCharArrayField("fChar", new[] {'a'})
                .SetIntArrayField("fInt", new[] {3})
                .SetLongArrayField("fLong", new long[] {4})
                .SetFloatArrayField("fFloat", new float[] {5})
                .SetDoubleArrayField("fDouble", new double[] {6})
                .SetDecimalArrayField("fDecimal", new decimal?[] {7.7m})
                .SetHashCode(100)
                .Build();

            CheckPrimitiveArrayFields1(binObj2);

            // Check equality.
            Assert.AreEqual(binObj, binObj2);
            Assert.AreEqual(binObj.GetHashCode(), binObj2.GetHashCode());

            // Overwrite with generic setter.
            binObj = _grid.GetBinary().GetBuilder(binObj)
                .SetField("fByte", new byte[] { 7 })
                .SetField("fBool", new[] { false })
                .SetField("fShort", new short[] { 8 })
                .SetField("fChar", new[] { 'b' })
                .SetField("fInt", new[] { 9 })
                .SetField("fLong", new long[] { 10 })
                .SetField("fFloat", new float[] { 11 })
                .SetField("fDouble", new double[] { 12 })
                .SetField("fDecimal", new decimal?[] { 13.13m })
                .Build();

            CheckPrimitiveArrayFields2(binObj);

            // Overwrite with specific setters.
            binObj2 = _grid.GetBinary().GetBuilder(binObj)
                .SetByteArrayField("fByte", new byte[] { 7 })
                .SetBooleanArrayField("fBool", new[] { false })
                .SetShortArrayField("fShort", new short[] { 8 })
                .SetCharArrayField("fChar", new[] { 'b' })
                .SetIntArrayField("fInt", new[] { 9 })
                .SetLongArrayField("fLong", new long[] { 10 })
                .SetFloatArrayField("fFloat", new float[] { 11 })
                .SetDoubleArrayField("fDouble", new double[] { 12 })
                .SetDecimalArrayField("fDecimal", new decimal?[] { 13.13m })
                .Build();

            CheckPrimitiveArrayFields2(binObj);
            
            // Check equality.
            Assert.AreEqual(binObj, binObj2);
            Assert.AreEqual(binObj.GetHashCode(), binObj2.GetHashCode());
        }

        /// <summary>
        /// Checks the primitive array fields.
        /// </summary>
        private static void CheckPrimitiveArrayFields1(IBinaryObject binObj)
        {
            Assert.AreEqual(100, binObj.GetHashCode());

            IBinaryType meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(PrimitiveArrays).Name, meta.TypeName);

            Assert.AreEqual(9, meta.Fields.Count);

            Assert.AreEqual(BinaryTypeNames.TypeNameArrayByte, meta.GetFieldTypeName("fByte"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayBool, meta.GetFieldTypeName("fBool"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayShort, meta.GetFieldTypeName("fShort"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayChar, meta.GetFieldTypeName("fChar"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayInt, meta.GetFieldTypeName("fInt"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayLong, meta.GetFieldTypeName("fLong"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayFloat, meta.GetFieldTypeName("fFloat"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayDouble, meta.GetFieldTypeName("fDouble"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayDecimal, meta.GetFieldTypeName("fDecimal"));

            Assert.AreEqual(new byte[] { 1 }, binObj.GetField<byte[]>("fByte"));
            Assert.AreEqual(new[] { true }, binObj.GetField<bool[]>("fBool"));
            Assert.AreEqual(new short[] { 2 }, binObj.GetField<short[]>("fShort"));
            Assert.AreEqual(new[] { 'a' }, binObj.GetField<char[]>("fChar"));
            Assert.AreEqual(new[] { 3 }, binObj.GetField<int[]>("fInt"));
            Assert.AreEqual(new long[] { 4 }, binObj.GetField<long[]>("fLong"));
            Assert.AreEqual(new float[] { 5 }, binObj.GetField<float[]>("fFloat"));
            Assert.AreEqual(new double[] { 6 }, binObj.GetField<double[]>("fDouble"));
            Assert.AreEqual(new decimal?[] { 7.7m }, binObj.GetField<decimal?[]>("fDecimal"));

            PrimitiveArrays obj = binObj.Deserialize<PrimitiveArrays>();

            Assert.AreEqual(new byte[] { 1 }, obj.FByte);
            Assert.AreEqual(new[] { true }, obj.FBool);
            Assert.AreEqual(new short[] { 2 }, obj.FShort);
            Assert.AreEqual(new[] { 'a' }, obj.FChar);
            Assert.AreEqual(new[] { 3 }, obj.FInt);
            Assert.AreEqual(new long[] { 4 }, obj.FLong);
            Assert.AreEqual(new float[] { 5 }, obj.FFloat);
            Assert.AreEqual(new double[] { 6 }, obj.FDouble);
            Assert.AreEqual(new decimal?[] { 7.7m }, obj.FDecimal);
        }

        /// <summary>
        /// Checks the primitive array fields.
        /// </summary>
        private static void CheckPrimitiveArrayFields2(IBinaryObject binObj)
        {
            Assert.AreEqual(new byte[] { 7 }, binObj.GetField<byte[]>("fByte"));
            Assert.AreEqual(new[] { false }, binObj.GetField<bool[]>("fBool"));
            Assert.AreEqual(new short[] { 8 }, binObj.GetField<short[]>("fShort"));
            Assert.AreEqual(new[] { 'b' }, binObj.GetField<char[]>("fChar"));
            Assert.AreEqual(new[] { 9 }, binObj.GetField<int[]>("fInt"));
            Assert.AreEqual(new long[] { 10 }, binObj.GetField<long[]>("fLong"));
            Assert.AreEqual(new float[] { 11 }, binObj.GetField<float[]>("fFloat"));
            Assert.AreEqual(new double[] { 12 }, binObj.GetField<double[]>("fDouble"));
            Assert.AreEqual(new decimal?[] { 13.13m }, binObj.GetField<decimal?[]>("fDecimal"));

            var obj = binObj.Deserialize<PrimitiveArrays>();

            Assert.AreEqual(new byte[] { 7 }, obj.FByte);
            Assert.AreEqual(new[] { false }, obj.FBool);
            Assert.AreEqual(new short[] { 8 }, obj.FShort);
            Assert.AreEqual(new[] { 'b' }, obj.FChar);
            Assert.AreEqual(new[] { 9 }, obj.FInt);
            Assert.AreEqual(new long[] { 10 }, obj.FLong);
            Assert.AreEqual(new float[] { 11 }, obj.FFloat);
            Assert.AreEqual(new double[] { 12 }, obj.FDouble);
            Assert.AreEqual(new decimal?[] { 13.13m }, obj.FDecimal);
        }

        /// <summary>
        /// Test non-primitive fields and their array counterparts.
        /// </summary>
        [Test]
        public void TestStringDateGuidEnum()
        {
            DateTime? nDate = DateTime.Now.ToUniversalTime();

            Guid? nGuid = Guid.NewGuid();

            // Generic setters.
            var binObj = _grid.GetBinary().GetBuilder(typeof(StringDateGuidEnum))
                .SetField("fStr", "str")
                .SetField("fNDate", nDate)
                .SetTimestampField("fNTimestamp", nDate)
                .SetField("fNGuid", nGuid)
                .SetField("fEnum", TestEnum.One)
                .SetStringArrayField("fStrArr", new[] { "str" })
                .SetArrayField("fDateArr", new[] { nDate })
                .SetTimestampArrayField("fTimestampArr", new[] { nDate })
                .SetGuidArrayField("fGuidArr", new[] { nGuid })
                .SetEnumArrayField("fEnumArr", new[] { TestEnum.One })
                .SetHashCode(100)
                .Build();

            CheckStringDateGuidEnum1(binObj, nDate, nGuid);

            // Specific setters.
            var binObj2 = _grid.GetBinary().GetBuilder(typeof(StringDateGuidEnum))
                .SetStringField("fStr", "str")
                .SetField("fNDate", nDate)
                .SetTimestampField("fNTimestamp", nDate)
                .SetGuidField("fNGuid", nGuid)
                .SetEnumField("fEnum", TestEnum.One)
                .SetStringArrayField("fStrArr", new[] { "str" })
                .SetArrayField("fDateArr", new[] { nDate })
                .SetTimestampArrayField("fTimestampArr", new[] { nDate })
                .SetGuidArrayField("fGuidArr", new[] { nGuid })
                .SetEnumArrayField("fEnumArr", new[] { TestEnum.One })
                .SetHashCode(100)
                .Build();

            CheckStringDateGuidEnum1(binObj2, nDate, nGuid);

            // Check equality.
            Assert.AreEqual(binObj, binObj2);
            Assert.AreEqual(binObj.GetHashCode(), binObj2.GetHashCode());

            // Overwrite.
            nDate = DateTime.Now.ToUniversalTime();
            nGuid = Guid.NewGuid();

            binObj = _grid.GetBinary().GetBuilder(typeof(StringDateGuidEnum))
                .SetField("fStr", "str2")
                .SetField("fNDate", nDate)
                .SetTimestampField("fNTimestamp", nDate)
                .SetField("fNGuid", nGuid)
                .SetField("fEnum", TestEnum.Two)
                .SetField("fStrArr", new[] { "str2" })
                .SetArrayField("fDateArr", new[] { nDate })
                .SetTimestampArrayField("fTimestampArr", new[] { nDate })
                .SetField("fGuidArr", new[] { nGuid })
                .SetField("fEnumArr", new[] { TestEnum.Two })
                .Build();

            CheckStringDateGuidEnum2(binObj, nDate, nGuid);

            // Overwrite with specific setters
            binObj2 = _grid.GetBinary().GetBuilder(typeof(StringDateGuidEnum))
                .SetStringField("fStr", "str2")
                .SetField("fNDate", nDate)
                .SetTimestampField("fNTimestamp", nDate)
                .SetGuidField("fNGuid", nGuid)
                .SetEnumField("fEnum", TestEnum.Two)
                .SetStringArrayField("fStrArr", new[] { "str2" })
                .SetArrayField("fDateArr", new[] { nDate })
                .SetTimestampArrayField("fTimestampArr", new[] { nDate })
                .SetGuidArrayField("fGuidArr", new[] { nGuid })
                .SetEnumArrayField("fEnumArr", new[] { TestEnum.Two })
                .Build();

            CheckStringDateGuidEnum2(binObj2, nDate, nGuid);

            // Check equality.
            Assert.AreEqual(binObj, binObj2);
            Assert.AreEqual(binObj.GetHashCode(), binObj2.GetHashCode());
        }

        /// <summary>
        /// Checks the string date guid enum values.
        /// </summary>
        private void CheckStringDateGuidEnum1(IBinaryObject binObj, DateTime? nDate, Guid? nGuid)
        {
            Assert.AreEqual(100, binObj.GetHashCode());

            IBinaryType meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(StringDateGuidEnum).Name, meta.TypeName);

            Assert.AreEqual(10, meta.Fields.Count);

            Assert.AreEqual(BinaryTypeNames.TypeNameString, meta.GetFieldTypeName("fStr"));
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, meta.GetFieldTypeName("fNDate"));
            Assert.AreEqual(BinaryTypeNames.TypeNameTimestamp, meta.GetFieldTypeName("fNTimestamp"));
            Assert.AreEqual(BinaryTypeNames.TypeNameGuid, meta.GetFieldTypeName("fNGuid"));
            Assert.AreEqual(BinaryTypeNames.TypeNameEnum, meta.GetFieldTypeName("fEnum"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayString, meta.GetFieldTypeName("fStrArr"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("fDateArr"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayTimestamp, meta.GetFieldTypeName("fTimestampArr"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayGuid, meta.GetFieldTypeName("fGuidArr"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayEnum, meta.GetFieldTypeName("fEnumArr"));

            Assert.AreEqual("str", binObj.GetField<string>("fStr"));
            Assert.AreEqual(nDate, binObj.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nDate, binObj.GetField<DateTime?>("fNTimestamp"));
            Assert.AreEqual(nGuid, binObj.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.One, binObj.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] {"str"}, binObj.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] {nDate}, binObj.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] {nDate}, binObj.GetField<DateTime?[]>("fTimestampArr"));
            Assert.AreEqual(new[] {nGuid}, binObj.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] {TestEnum.One}, binObj.GetField<TestEnum[]>("fEnumArr"));

            StringDateGuidEnum obj = binObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str", obj.FStr);
            Assert.AreEqual(nDate, obj.FnDate);
            Assert.AreEqual(nDate, obj.FnTimestamp);
            Assert.AreEqual(nGuid, obj.FnGuid);
            Assert.AreEqual(TestEnum.One, obj.FEnum);
            Assert.AreEqual(new[] {"str"}, obj.FStrArr);
            Assert.AreEqual(new[] {nDate}, obj.FDateArr);
            Assert.AreEqual(new[] {nGuid}, obj.FGuidArr);
            Assert.AreEqual(new[] {TestEnum.One}, obj.FEnumArr);

            // Check builder field caching.
            var builder = _grid.GetBinary().GetBuilder(binObj);

            Assert.AreEqual("str", builder.GetField<string>("fStr"));
            Assert.AreEqual(nDate, builder.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nDate, builder.GetField<DateTime?>("fNTimestamp"));
            Assert.AreEqual(nGuid, builder.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.One, builder.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] {"str"}, builder.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] {nDate}, builder.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] {nDate}, builder.GetField<DateTime?[]>("fTimestampArr"));
            Assert.AreEqual(new[] {nGuid}, builder.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] {TestEnum.One}, builder.GetField<TestEnum[]>("fEnumArr"));

            // Check reassemble.
            binObj = builder.Build();

            Assert.AreEqual("str", binObj.GetField<string>("fStr"));
            Assert.AreEqual(nDate, binObj.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nDate, binObj.GetField<DateTime?>("fNTimestamp"));
            Assert.AreEqual(nGuid, binObj.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.One, binObj.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] {"str"}, binObj.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] {nDate}, binObj.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] {nDate}, binObj.GetField<DateTime?[]>("fTimestampArr"));
            Assert.AreEqual(new[] {nGuid}, binObj.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] {TestEnum.One}, binObj.GetField<TestEnum[]>("fEnumArr"));

            obj = binObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str", obj.FStr);
            Assert.AreEqual(nDate, obj.FnDate);
            Assert.AreEqual(nDate, obj.FnTimestamp);
            Assert.AreEqual(nGuid, obj.FnGuid);
            Assert.AreEqual(TestEnum.One, obj.FEnum);
            Assert.AreEqual(new[] {"str"}, obj.FStrArr);
            Assert.AreEqual(new[] {nDate}, obj.FDateArr);
            Assert.AreEqual(new[] {nGuid}, obj.FGuidArr);
            Assert.AreEqual(new[] {TestEnum.One}, obj.FEnumArr);
        }

        /// <summary>
        /// Checks the string date guid enum values.
        /// </summary>
        private static void CheckStringDateGuidEnum2(IBinaryObject binObj, DateTime? nDate, Guid? nGuid)
        {
            Assert.AreEqual("str2", binObj.GetField<string>("fStr"));
            Assert.AreEqual(nDate, binObj.GetField<DateTime?>("fNDate"));
            Assert.AreEqual(nDate, binObj.GetField<DateTime?>("fNTimestamp"));
            Assert.AreEqual(nGuid, binObj.GetField<Guid?>("fNGuid"));
            Assert.AreEqual(TestEnum.Two, binObj.GetField<TestEnum>("fEnum"));
            Assert.AreEqual(new[] { "str2" }, binObj.GetField<string[]>("fStrArr"));
            Assert.AreEqual(new[] { nDate }, binObj.GetField<DateTime?[]>("fDateArr"));
            Assert.AreEqual(new[] { nDate }, binObj.GetField<DateTime?[]>("fTimestampArr"));
            Assert.AreEqual(new[] { nGuid }, binObj.GetField<Guid?[]>("fGuidArr"));
            Assert.AreEqual(new[] { TestEnum.Two }, binObj.GetField<TestEnum[]>("fEnumArr"));

            var obj = binObj.Deserialize<StringDateGuidEnum>();

            Assert.AreEqual("str2", obj.FStr);
            Assert.AreEqual(nDate, obj.FnDate);
            Assert.AreEqual(nDate, obj.FnTimestamp);
            Assert.AreEqual(nGuid, obj.FnGuid);
            Assert.AreEqual(TestEnum.Two, obj.FEnum);
            Assert.AreEqual(new[] { "str2" }, obj.FStrArr);
            Assert.AreEqual(new[] { nDate }, obj.FDateArr);
            Assert.AreEqual(new[] { nGuid }, obj.FGuidArr);
            Assert.AreEqual(new[] { TestEnum.Two }, obj.FEnumArr);
        }

        [Test]
        public void TestEnumMeta()
        {
            var bin = _grid.GetBinary();

            // Put to cache to populate metas
            var binEnum = bin.ToBinary<IBinaryObject>(TestEnumRegistered.One);

            Assert.AreEqual(_marsh.GetDescriptor(typeof (TestEnumRegistered)).TypeId, binEnum.GetBinaryType().TypeId);
            Assert.AreEqual(0, binEnum.EnumValue);

            var meta = binEnum.GetBinaryType();

            Assert.IsTrue(meta.IsEnum);
            Assert.AreEqual(typeof (TestEnumRegistered).Name, meta.TypeName);
            Assert.AreEqual(0, meta.Fields.Count);
        }

        /// <summary>
        /// Test arrays.
        /// </summary>
        [Test]
        public void TestCompositeArray()
        {
            // 1. Test simple array.
            object[] inArr = { new CompositeInner(1) };

            IBinaryObject binObj = _grid.GetBinary().GetBuilder(typeof(CompositeArray)).SetHashCode(100)
                .SetField("inArr", inArr).Build();

            IBinaryType meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(CompositeArray).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("inArr"));

            Assert.AreEqual(100, binObj.GetHashCode());

            var binInArr = binObj.GetField<IBinaryObject[]>("inArr").ToArray();

            Assert.AreEqual(1, binInArr.Length);
            Assert.AreEqual(1, binInArr[0].GetField<int>("val"));

            CompositeArray arr = binObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(1, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner) arr.InArr[0]).Val);

            // 2. Test addition to array.
            binObj = _grid.GetBinary().GetBuilder(binObj).SetHashCode(200)
                .SetField("inArr", new[] { binInArr[0], null }).Build();

            Assert.AreEqual(200, binObj.GetHashCode());

            binInArr = binObj.GetField<IBinaryObject[]>("inArr").ToArray();

            Assert.AreEqual(2, binInArr.Length);
            Assert.AreEqual(1, binInArr[0].GetField<int>("val"));
            Assert.IsNull(binInArr[1]);

            arr = binObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner) arr.InArr[0]).Val);
            Assert.IsNull(arr.InArr[1]);

            binInArr[1] = _grid.GetBinary().GetBuilder(typeof(CompositeInner)).SetField("val", 2).Build();

            binObj = _grid.GetBinary().GetBuilder(binObj).SetHashCode(300)
                .SetField("inArr", binInArr.OfType<object>().ToArray()).Build();

            Assert.AreEqual(300, binObj.GetHashCode());

            binInArr = binObj.GetField<IBinaryObject[]>("inArr").ToArray();

            Assert.AreEqual(2, binInArr.Length);
            Assert.AreEqual(1, binInArr[0].GetField<int>("val"));
            Assert.AreEqual(2, binInArr[1].GetField<int>("val"));

            arr = binObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[0]).Val);
            Assert.AreEqual(2, ((CompositeInner)arr.InArr[1]).Val);

            // 3. Test top-level handle inversion.
            CompositeInner inner = new CompositeInner(1);

            inArr = new object[] { inner, inner };

            binObj = _grid.GetBinary().GetBuilder(typeof(CompositeArray)).SetHashCode(100)
                .SetField("inArr", inArr).Build();

            Assert.AreEqual(100, binObj.GetHashCode());

            binInArr = binObj.GetField<IBinaryObject[]>("inArr").ToArray();

            Assert.AreEqual(2, binInArr.Length);
            Assert.AreEqual(1, binInArr[0].GetField<int>("val"));
            Assert.AreEqual(1, binInArr[1].GetField<int>("val"));

            arr = binObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[0]).Val);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[1]).Val);

            binInArr[0] = _grid.GetBinary().GetBuilder(typeof(CompositeInner)).SetField("val", 2).Build();

            binObj = _grid.GetBinary().GetBuilder(binObj).SetHashCode(200)
                .SetField("inArr", binInArr.ToArray<object>()).Build();

            Assert.AreEqual(200, binObj.GetHashCode());

            binInArr = binObj.GetField<IBinaryObject[]>("inArr").ToArray();

            Assert.AreEqual(2, binInArr.Length);
            Assert.AreEqual(2, binInArr[0].GetField<int>("val"));
            Assert.AreEqual(1, binInArr[1].GetField<int>("val"));

            arr = binObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.OutArr);
            Assert.AreEqual(2, arr.InArr.Length);
            Assert.AreEqual(2, ((CompositeInner)arr.InArr[0]).Val);
            Assert.AreEqual(1, ((CompositeInner)arr.InArr[1]).Val);

            // 4. Test nested object handle inversion.
            CompositeOuter[] outArr = { new CompositeOuter(inner), new CompositeOuter(inner) };

            binObj = _grid.GetBinary().GetBuilder(typeof(CompositeArray)).SetHashCode(100)
                .SetField("outArr", outArr.ToArray<object>()).Build();

            meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(CompositeArray).Name, meta.TypeName);
            Assert.AreEqual(2, meta.Fields.Count);
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("inArr"));
            Assert.AreEqual(BinaryTypeNames.TypeNameArrayObject, meta.GetFieldTypeName("outArr"));

            Assert.AreEqual(100, binObj.GetHashCode());

            var binOutArr = binObj.GetField<IBinaryObject[]>("outArr").ToArray();

            Assert.AreEqual(2, binOutArr.Length);
            Assert.AreEqual(1, binOutArr[0].GetField<IBinaryObject>("inner").GetField<int>("val"));
            Assert.AreEqual(1, binOutArr[1].GetField<IBinaryObject>("inner").GetField<int>("val"));

            arr = binObj.Deserialize<CompositeArray>();

            Assert.IsNull(arr.InArr);
            Assert.AreEqual(2, arr.OutArr.Length);
            Assert.AreEqual(1, ((CompositeOuter) arr.OutArr[0]).Inner.Val);
            Assert.AreEqual(1, ((CompositeOuter) arr.OutArr[0]).Inner.Val);

            binOutArr[0] = _grid.GetBinary().GetBuilder(typeof(CompositeOuter))
                .SetField("inner", new CompositeInner(2)).Build();

            binObj = _grid.GetBinary().GetBuilder(binObj).SetHashCode(200)
                .SetField("outArr", binOutArr.ToArray<object>()).Build();

            Assert.AreEqual(200, binObj.GetHashCode());

            binInArr = binObj.GetField<IBinaryObject[]>("outArr").ToArray();

            Assert.AreEqual(2, binInArr.Length);
            Assert.AreEqual(2, binOutArr[0].GetField<IBinaryObject>("inner").GetField<int>("val"));
            Assert.AreEqual(1, binOutArr[1].GetField<IBinaryObject>("inner").GetField<int>("val"));

            arr = binObj.Deserialize<CompositeArray>();

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

            IBinaryObject binObj = _grid.GetBinary().GetBuilder(typeof(CompositeContainer)).SetHashCode(100)
                .SetCollectionField("col", col)
                .SetDictionaryField("dict", dict).Build();

            // 1. Check meta.
            IBinaryType meta = binObj.GetBinaryType();

            Assert.AreEqual(typeof(CompositeContainer).Name, meta.TypeName);

            Assert.AreEqual(2, meta.Fields.Count);
            Assert.AreEqual(BinaryTypeNames.TypeNameCollection, meta.GetFieldTypeName("col"));
            Assert.AreEqual(BinaryTypeNames.TypeNameMap, meta.GetFieldTypeName("dict"));

            // 2. Check in binary form.
            Assert.AreEqual(1, binObj.GetField<ICollection>("col").Count);
            Assert.AreEqual(1, binObj.GetField<ICollection>("col").OfType<IBinaryObject>().First()
                .GetField<int>("val"));

            Assert.AreEqual(1, binObj.GetField<IDictionary>("dict").Count);
            Assert.AreEqual(3, ((IBinaryObject) binObj.GetField<IDictionary>("dict")[3]).GetField<int>("val"));

            // 3. Check in deserialized form.
            CompositeContainer obj = binObj.Deserialize<CompositeContainer>();

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

            var binObj = _marsh.Unmarshal<IBinaryObject>(_marsh.Marshal(raw), BinaryMode.ForceBinary);

            raw = binObj.Deserialize<WithRaw>();

            Assert.AreEqual(1, raw.A);
            Assert.AreEqual(2, raw.B);

            IBinaryObject newbinObj = _grid.GetBinary().GetBuilder(binObj).SetField("a", 3).Build();

            raw = newbinObj.Deserialize<WithRaw>();

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
            IBinaryObjectBuilder builder = _grid.GetBinary().GetBuilder(typeof(NestedOuter));

            NestedInner inner1 = new NestedInner {Val = 1};
            builder.SetField("inner1", inner1);

            IBinaryObject outerbinObj = builder.Build();

            IBinaryType meta = outerbinObj.GetBinaryType();

            Assert.AreEqual(typeof(NestedOuter).Name, meta.TypeName);
            Assert.AreEqual(1, meta.Fields.Count);
            Assert.AreEqual(BinaryTypeNames.TypeNameObject, meta.GetFieldTypeName("inner1"));

            IBinaryObject innerbinObj1 = outerbinObj.GetField<IBinaryObject>("inner1");

            IBinaryType innerMeta = innerbinObj1.GetBinaryType();

            Assert.AreEqual(typeof(NestedInner).Name, innerMeta.TypeName);
            Assert.AreEqual(1, innerMeta.Fields.Count);
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, innerMeta.GetFieldTypeName("Val"));

            inner1 = innerbinObj1.Deserialize<NestedInner>();

            Assert.AreEqual(1, inner1.Val);

            NestedOuter outer = outerbinObj.Deserialize<NestedOuter>();
            Assert.AreEqual(outer.Inner1.Val, 1);
            Assert.IsNull(outer.Inner2);

            // 2. Add another field over existing binary object.
            builder = _grid.GetBinary().GetBuilder(outerbinObj);

            NestedInner inner2 = new NestedInner {Val = 2};
            builder.SetField("inner2", inner2);

            outerbinObj = builder.Build();

            outer = outerbinObj.Deserialize<NestedOuter>();
            Assert.AreEqual(1, outer.Inner1.Val);
            Assert.AreEqual(2, outer.Inner2.Val);

            // 3. Try setting inner object in binary form.
            innerbinObj1 = _grid.GetBinary().GetBuilder(innerbinObj1).SetField("val", 3).Build();

            inner1 = innerbinObj1.Deserialize<NestedInner>();

            Assert.AreEqual(3, inner1.Val);

            outerbinObj = _grid.GetBinary().GetBuilder(outerbinObj).SetField<object>("inner1", innerbinObj1).Build();

            outer = outerbinObj.Deserialize<NestedOuter>();
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

            IBinaryObjectBuilder builder = _grid.GetBinary().GetBuilder(typeof(MigrationOuter));

            if (GetIdentityResolver() == null)
                builder.SetHashCode(outer.GetHashCode());

            builder.SetField<object>("inner1", inner);
            builder.SetField<object>("inner2", inner);

            BinaryObject portOuter = (BinaryObject) builder.Build();

            byte[] portOuterBytes = new byte[outerBytes.Length];

            Buffer.BlockCopy(portOuter.Data, 0, portOuterBytes, 0, portOuterBytes.Length);

            Assert.AreEqual(outerBytes, portOuterBytes);

            // 2. Change the first inner object so that the handle must migrate.
            MigrationInner inner1 = new MigrationInner {Val = 2};

            IBinaryObject portOuterMigrated =
                _grid.GetBinary().GetBuilder(portOuter).SetField<object>("inner1", inner1).Build();

            MigrationOuter outerMigrated = portOuterMigrated.Deserialize<MigrationOuter>();

            Assert.AreEqual(2, outerMigrated.Inner1.Val);
            Assert.AreEqual(1, outerMigrated.Inner2.Val);

            // 3. Change the first value using serialized form.
            IBinaryObject inner1Port =
                _grid.GetBinary().GetBuilder(typeof(MigrationInner)).SetField("val", 2).Build();

            portOuterMigrated =
                _grid.GetBinary().GetBuilder(portOuter).SetField<object>("inner1", inner1Port).Build();

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

            IBinaryObject portOuter = _marsh.Unmarshal<IBinaryObject>(rawOuter, BinaryMode.ForceBinary);
            IBinaryObject portInner = portOuter.GetField<IBinaryObject>("inner");

            // 1. Ensure that inner object can be deserialized after build.
            IBinaryObject portInnerNew = _grid.GetBinary().GetBuilder(portInner).Build();

            InversionInner innerNew = portInnerNew.Deserialize<InversionInner>();

            Assert.AreSame(innerNew, innerNew.Outer.Inner);

            // 2. Ensure that binary object with external dependencies could be added to builder.
            IBinaryObject portOuterNew =
                _grid.GetBinary().GetBuilder(typeof(InversionOuter)).SetField<object>("inner", portInner).Build();

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
            IBinaryObjectBuilder builder = _grid.GetBinary().GetBuilder(typeof(Primitives));

            builder.SetField<byte>("fByte", 1).SetField("fBool", true);

            IBinaryObject po1 = builder.Build();
            IBinaryObject po2 = builder.Build();

            Assert.AreEqual(1, po1.GetField<byte>("fByte"));
            Assert.AreEqual(true, po1.GetField<bool>("fBool"));

            Assert.AreEqual(1, po2.GetField<byte>("fByte"));
            Assert.AreEqual(true, po2.GetField<bool>("fBool"));

            builder.SetField<byte>("fByte", 2);

            IBinaryObject po3 = builder.Build();

            Assert.AreEqual(1, po1.GetField<byte>("fByte"));
            Assert.AreEqual(true, po1.GetField<bool>("fBool"));

            Assert.AreEqual(1, po2.GetField<byte>("fByte"));
            Assert.AreEqual(true, po2.GetField<bool>("fBool"));

            Assert.AreEqual(2, po3.GetField<byte>("fByte"));
            Assert.AreEqual(true, po2.GetField<bool>("fBool"));

            builder = _grid.GetBinary().GetBuilder(po1);

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
            Assert.Throws<ArgumentException>(() => _grid.GetBinary().GetTypeId(null));

            Assert.AreEqual(IdMapper.TestTypeId, _grid.GetBinary().GetTypeId(IdMapper.TestTypeName));
            
            Assert.AreEqual(BinaryUtils.GetStringHashCode("someTypeName"), _grid.GetBinary().GetTypeId("someTypeName"));
        }

        /// <summary>
        /// Tests type name mapper.
        /// </summary>
        [Test]
        public void TestTypeName()
        {
            var bytes = _marsh.Marshal(new NameMapperTestType {NameMapperTestField = 17});

            var bin = _marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            var binType = bin.GetBinaryType();

            Assert.AreEqual(BinaryUtils.GetStringHashCode(NameMapper.TestTypeName + "_"), binType.TypeId);
            Assert.AreEqual(17, bin.GetField<int>(NameMapper.TestFieldName));
        }

        /// <summary>
        /// Tests metadata methods.
        /// </summary>
        [Test]
        public void TestMetadata()
        {
            // Populate metadata
            var binary = _grid.GetBinary();

            binary.ToBinary<IBinaryObject>(new DecimalHolder());

            // All meta
            var allMetas = binary.GetBinaryTypes();

            var decimalMeta = allMetas.Single(x => x.TypeName == "DecimalHolder");

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);

            // By type
            decimalMeta = binary.GetBinaryType(typeof (DecimalHolder));

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);
            
            // By type id
            decimalMeta = binary.GetBinaryType(binary.GetTypeId("DecimalHolder"));

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);

            // By type name
            decimalMeta = binary.GetBinaryType("DecimalHolder");

            Assert.AreEqual(new[] {"val", "valArr"}, decimalMeta.Fields);
        }

        [Test]
        public void TestBuildEnum()
        {
            var binary = _grid.GetBinary();

            int val = (int) TestEnumRegistered.Two;

            var binEnums = new[]
            {
                binary.BuildEnum(typeof (TestEnumRegistered), val),
                binary.BuildEnum(typeof (TestEnumRegistered).Name, val)
            };

            foreach (var binEnum in binEnums)
            {
                Assert.IsTrue(binEnum.GetBinaryType().IsEnum);

                Assert.AreEqual(val, binEnum.EnumValue);

                Assert.AreEqual((TestEnumRegistered) val, binEnum.Deserialize<TestEnumRegistered>());
            }

            Assert.AreEqual(binEnums[0], binEnums[1]);
            Assert.AreEqual(binEnums[0].GetHashCode(), binEnums[1].GetHashCode());
        }

        /// <summary>
        /// Tests the compact footer setting.
        /// </summary>
        [Test]
        public void TestCompactFooterSetting()
        {
            Assert.AreEqual(GetCompactFooter(), _marsh.CompactFooter);
        }

        /// <summary>
        /// Tests the binary mode on remote node.
        /// </summary>
        [Test]
        public void TestRemoteBinaryMode()
        {
            if (GetIdentityResolver() != null)
                return;  // When identity resolver is set, it is required to have the same config on all nodes.

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                GridName = "grid2",
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = GetCompactFooter()
                }
            };

            using (var grid2 = Ignition.Start(cfg))
            {
                var cache1 = _grid.GetOrCreateCache<int, Primitives>("cache");
                var cache2 = grid2.GetCache<int, object>("cache").WithKeepBinary<int, IBinaryObject>();

                // Exchange data
                cache1[1] = new Primitives {FByte = 3};
                var obj = cache2[1];

                // Rebuild with no changes
                cache2[2] = obj.ToBuilder().Build();
                Assert.AreEqual(3, cache1[2].FByte);

                // Rebuild with read
                Assert.AreEqual(3, obj.GetField<byte>("FByte"));
                cache2[3] = obj.ToBuilder().Build();
                Assert.AreEqual(3, cache1[3].FByte);

                // Modify and rebuild
                cache2[4] = obj.ToBuilder().SetField("FShort", (short) 15).Build();
                Assert.AreEqual(15, cache1[4].FShort);

                // New binary type without a class
                cache2[5] = grid2.GetBinary().GetBuilder("myNewType").SetField("foo", "bar").Build();

                var cache1Bin = cache1.WithKeepBinary<int, IBinaryObject>();
                var newObj = cache1Bin[5];
                Assert.AreEqual("bar", newObj.GetField<string>("foo"));

                cache1Bin[6] = newObj.ToBuilder().SetField("foo2", 3).Build();
                Assert.AreEqual(3, cache2[6].GetField<int>("foo2"));
            }
        }
    }

    /// <summary>
    /// Empty binary class.
    /// </summary>
    public class Empty
    {
        // No-op.
    }

    /// <summary>
    /// binary with primitive fields.
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
        public decimal FDecimal;
    }

    /// <summary>
    /// binary with primitive array fields.
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
        public decimal?[] FDecimal;
    }

    /// <summary>
    /// binary having strings, dates, Guids and enums.
    /// </summary>
    public class StringDateGuidEnum
    {
        public string FStr;
        public DateTime? FnDate;
        public DateTime? FnTimestamp;
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
    /// Registered enumeration.
    /// </summary>
    public enum TestEnumRegistered
    {
        One, Two
    }

    /// <summary>
    /// binary with raw data.
    /// </summary>
    public class WithRaw : IBinarizable
    {
        public int A;
        public int B;

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("a", A);
            writer.GetRawWriter().WriteInt(B);
        }

        /** <inheritDoc /> */
        public void ReadBinary(IBinaryReader reader)
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
    /// Type to test "ToBinary()" logic.
    /// </summary>
    public class ToBinary
    {
        public int Val;

        public ToBinary(int val)
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
    public class IdMapper : IBinaryIdMapper
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

    /// <summary>
    /// Test name mapper.
    /// </summary>
    public class NameMapper : IBinaryNameMapper
    {
        /** */
        public const string TestTypeName = "NameMapperTestType";

        /** */
        public const string TestFieldName = "NameMapperTestField";

        /** <inheritdoc /> */
        public string GetTypeName(string name)
        {
            if (name == TestTypeName)
                return name + "_";

            return name;
        }

        /** <inheritdoc /> */
        public string GetFieldName(string name)
        {
            if (name == TestFieldName)
                return name + "_";

            return name;
        }
    }

    /// <summary>
    /// Name mapper test type.
    /// </summary>
    public class NameMapperTestType
    {
        /** */
        public int NameMapperTestField { get; set; }
    }
}
