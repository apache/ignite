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
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// DateTime binary serialization tests.
    /// </summary>
    public class BinaryDateTimeTest
    {
        /** */
        internal const String FromErrMsg = "FromJavaTicks Error!";

        /** */
        internal const String ToErrMsg = "ToJavaTicks Error!";

        /** */
        private const String NotUtcDate =
            "DateTime is not UTC. Only UTC DateTime can be used for interop with other platforms.";

        /// <summary>
        /// Sets up the test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration(typeof(DateTimeObj2))
                        {
                            Serializer = new BinaryReflectiveSerializer {ForceTimestamp = true}
                        }
                    }
                }
            });
        }

        /// <summary>
        /// Tears down the test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the default behavior: DateTime is written as ISerializable object.
        /// </summary>
        [Test]
        public void TestDefaultBehavior()
        {
            AssertDateTimeField<DateTimeObj>((o, d) => o.Value = d, o => o.Value, "Value");
        }

        /// <summary>
        /// Tests the ForceTimestamp option in serializer.
        /// </summary>
        [Test]
        public void TestSerializerForceTimestamp()
        {
            // Check config.
            var ser = Ignition.GetIgnite()
                .GetConfiguration()
                .BinaryConfiguration.TypeConfigurations
                .Select(x => x.Serializer)
                .OfType<BinaryReflectiveSerializer>()
                .Single();

            Assert.IsTrue(ser.ForceTimestamp);

            AssertTimestampField<DateTimeObj2>((o, d) => o.Value = d, o => o.Value, "Value");
        }

        /// <summary>
        /// Tests TimestampAttribute applied to class members.
        /// </summary>
        [Test]
        public void TestMemberAttributes()
        {
            AssertTimestampField<DateTimePropertyAttribute>((o, d) => o.Value = d, o => o.Value, "Value");

            AssertTimestampField<DateTimeFieldAttribute>((o, d) => o.Value = d, o => o.Value, "Value");

            AssertTimestampField<DateTimeQueryFieldAttribute>((o, d) => o.Value = d, o => o.Value, "Value");
        }

        /// <summary>
        /// Tests TimestampAttribute applied to entire class.
        /// </summary>
        [Test]
        public void TestClassAttributes()
        {
            AssertTimestampField<DateTimeClassAttribute>((o, d) => o.Value = d, o => o.Value, "Value");

            AssertTimestampField<DateTimeClassAttribute2>((o, d) => o.Value = d, o => o.Value, "Value");
        }

        /// <summary>
        /// Tests custom timestamp converter that modifies the values by adding one year on write and read.
        /// This test verifies that actual converted values are used by Ignite.
        /// </summary>
        [Test]
        public void TestAddYearTimestampConverter()
        {
            var cfg =  new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                AutoGenerateIgniteInstanceName = true,
                BinaryConfiguration = new BinaryConfiguration
                {
                    ForceTimestamp = true, 
                    TimestampConverter = new AddYearTimestampConverter()
                }
            };

            var ignite = Ignition.Start(cfg);

            var dt = DateTime.UtcNow;
            var expected = dt.AddYears(2);

            // Key & value.
            var cache = ignite.GetOrCreateCache<DateTime, DateTime>(TestUtils.TestName);
            cache[dt] = dt;

            var resEntry = cache.Single();
            
            Assert.AreEqual(expected, resEntry.Key);
            Assert.AreEqual(expected, resEntry.Value);
            
            // Key & value array.
            
            // Object field.
            var cache2 = ignite.GetOrCreateCache<DateTimePropertyAttribute, DateTimePropertyAttribute>(
                TestUtils.TestName);
            
            cache2.RemoveAll();
            
            var obj = new DateTimePropertyAttribute {Value = dt};
            cache2[obj] = obj;

            var resEntry2 = cache2.Single();
            Assert.AreEqual(expected, resEntry2.Key.Value);
            Assert.AreEqual(expected, resEntry2.Value.Value);
            
            // Object array field.
            var cache3 = ignite.GetOrCreateCache<DateTimeArr, DateTimeArr>(TestUtils.TestName);
            cache3.RemoveAll();
            
            var obj2 = new DateTimeArr {Value = new[]{dt}};
            cache3[obj2] = obj2;
            cache3[obj2] = obj2;

            var resEntry3 = cache3.Single();
            Assert.AreEqual(expected, resEntry3.Key.Value.Single());
            Assert.AreEqual(expected, resEntry3.Value.Value.Single());
        }

        /// <summary>
        /// Tests custom timestamp converter.
        /// </summary>
        [Test]
        public void TestCustomTimestampConverter()
        {
            var cfg =  new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "ignite-1"))
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    ForceTimestamp = true, 
                    TimestampConverter = new TimestampConverter()
                }
            };

            var ignite = Ignition.Start(cfg);
            var binary = ignite.GetBinary();
 
            // Check config.
            Assert.NotNull(ignite.GetConfiguration().BinaryConfiguration.TimestampConverter);

            AssertTimestampField<DateTimeObj2>((o, d) => o.Value = d, o => o.Value, "Value", ignite);

            var dt1 = new DateTime(1997, 8, 29, 0, 0, 0, DateTimeKind.Utc);

            var ex = Assert.Throws<BinaryObjectException>(() => binary.ToBinary<DateTime>(dt1));
            Assert.AreEqual(ToErrMsg, ex.Message);

            ex = Assert.Throws<BinaryObjectException>(() => binary.ToBinary<DateTime?[]>(new DateTime?[] {dt1}));
            Assert.AreEqual(ToErrMsg, ex.Message);

            var dt2 = new DateTime(1997, 8, 4, 0, 0, 0, DateTimeKind.Utc);

            ex = Assert.Throws<BinaryObjectException>(() => binary.ToBinary<DateTime>(dt2));
            Assert.AreEqual(FromErrMsg, ex.Message);

            ex = Assert.Throws<BinaryObjectException>(() => binary.ToBinary<DateTime?[]>(new DateTime?[] {dt2}));
            Assert.AreEqual(FromErrMsg, ex.Message);

            var datesCache = ignite.CreateCache<DateTime, DateTime>("dates");

            var check = new Action<DateTime, DateTime, String>((date1, date2, errMsg) =>
            {
                ex = Assert.Throws<BinaryObjectException>(() => datesCache.Put(date1, date2), "Timestamp fields should throw an error on non-UTC values");

                Assert.AreEqual(errMsg, ex.Message);
            });

            check.Invoke(DateTime.Now, DateTime.UtcNow, NotUtcDate);
            check.Invoke(DateTime.UtcNow, DateTime.Now, NotUtcDate);
            check.Invoke(dt1, DateTime.UtcNow, ToErrMsg);
            check.Invoke(DateTime.UtcNow, dt1, ToErrMsg);

            var now = DateTime.UtcNow;

            datesCache.Put(now, dt2);
            ex = Assert.Throws<BinaryObjectException>(() => datesCache.Get(now), "Timestamp fields should throw an error on non-UTC values");
            Assert.AreEqual(FromErrMsg, ex.Message);
            
            datesCache.Put(now, now);
            Assert.AreEqual(now, datesCache.Get(now));

            var datesObjCache = ignite.CreateCache<DateTimeObj, DateTimeObj>("datesObj");

            check = (date1, date2, errMsg) =>
            {
                ex = Assert.Throws<BinaryObjectException>(() => datesObjCache.Put(new DateTimeObj {Value = date1}, new DateTimeObj {Value = date2}),
                    "Timestamp fields should throw an error on non-UTC values");

                Assert.AreEqual(errMsg, ex.Message);
            };

            check.Invoke(DateTime.Now, DateTime.UtcNow, NotUtcDate);
            check.Invoke(DateTime.UtcNow, DateTime.Now, NotUtcDate);
            check.Invoke(dt1, DateTime.UtcNow, ToErrMsg);
            check.Invoke(DateTime.UtcNow, dt1, ToErrMsg);

            var nowObj = new DateTimeObj {Value = now};

            datesObjCache.Put(nowObj, new DateTimeObj {Value = dt2});
            ex = Assert.Throws<BinaryObjectException>(() => datesObjCache.Get(nowObj), "Timestamp fields should throw an error on non-UTC values");
            Assert.AreEqual(FromErrMsg, ex.Message);
            
            datesObjCache.Put(nowObj, nowObj);
            Assert.AreEqual(nowObj.Value, datesObjCache.Get(nowObj).Value);
        }

        /// <summary>
        /// Asserts that specified field is serialized as DateTime object.
        /// </summary>
        private static void AssertDateTimeField<T>(Action<T, DateTime> setValue,
            Func<T, DateTime> getValue, string fieldName) where T : new()
        {
            var binary = Ignition.GetIgnite().GetBinary();

            foreach (var dateTime in new[] { DateTime.Now, DateTime.UtcNow, DateTime.MinValue, DateTime.MaxValue })
            {
                var obj = new T();
                setValue(obj, dateTime);

                var bin = binary.ToBinary<IBinaryObject>(obj);
                var res = bin.Deserialize<T>();

                Assert.AreEqual(getValue(obj), getValue(res));
                Assert.AreEqual(getValue(obj), bin.GetField<IBinaryObject>(fieldName).Deserialize<DateTime>());
                Assert.AreEqual("Object", bin.GetBinaryType().GetFieldTypeName(fieldName));
            }
        }

        /// <summary>
        /// Asserts that specified field is serialized as Timestamp.
        /// </summary>
        private static void AssertTimestampField<T>(Action<T, DateTime> setValue,
            Func<T, DateTime> getValue, string fieldName, IIgnite ignite = null) where T : new()
        {
            // Non-UTC DateTime throws.
            var binary = ignite != null ? ignite.GetBinary() : Ignition.GetIgnite().GetBinary();

            var obj = new T();

            setValue(obj, DateTime.Now);

            var ex = Assert.Throws<BinaryObjectException>(() => binary.ToBinary<IBinaryObject>(obj), 
                "Timestamp fields should throw an error on non-UTC values");

            Assert.AreEqual(NotUtcDate, ex.Message);

            // UTC DateTime works.
            setValue(obj, DateTime.UtcNow);
            var bin = binary.ToBinary<IBinaryObject>(obj);
            var res = bin.Deserialize<T>();

            Assert.AreEqual(getValue(obj), getValue(res));
            Assert.AreEqual(getValue(obj), bin.GetField<DateTime>(fieldName));
            Assert.AreEqual("Timestamp", bin.GetBinaryType().GetFieldTypeName(fieldName));
        }

        private class DateTimeObj
        {
            public DateTime Value { get; set; }
        }

        private class DateTimeObj2
        {
            public DateTime Value { get; set; }
        }

        private class DateTimeArr
        {
            public DateTime[] Value { get; set; }
        }

        private class DateTimePropertyAttribute
        {
            [Timestamp]
            public DateTime Value { get; set; }
        }

        private class DateTimeFieldAttribute
        {
            [Timestamp]
            public DateTime Value;
        }

        private class DateTimeQueryFieldAttribute
        {
            [QuerySqlField]
            public DateTime Value { get; set; }
        }

        [Timestamp]
        private class DateTimeClassAttribute
        {
            public DateTime Value { get; set; }
        }

        [Timestamp]
        private class DateTimeClassAttribute2
        {
            public DateTime Value;
        }
        
        private class TimestampConverter : ITimestampConverter
        {
            /** <inheritdoc /> */
            public void ToJavaTicks(DateTime date, out long high, out int low)
            {
                if (date.Year == 1997 && date.Month == 8 && date.Day == 29)
                    throw new BinaryObjectException(BinaryDateTimeTest.ToErrMsg);

                BinaryUtils.ToJavaDate(date, out high, out low);
            }

            /** <inheritdoc /> */
            public DateTime FromJavaTicks(long high, int low)
            {
                var date = new DateTime(BinaryUtils.JavaDateTicks + high * TimeSpan.TicksPerMillisecond + low / 100,
                    DateTimeKind.Utc);

                if (date.Year == 1997 && date.Month == 8 && date.Day == 4)
                    throw new BinaryObjectException(BinaryDateTimeTest.FromErrMsg);

                return date;
            }
        }
        
        private class AddYearTimestampConverter : ITimestampConverter
        {
            /** <inheritdoc /> */
            public void ToJavaTicks(DateTime date, out long high, out int low)
            {
                BinaryUtils.ToJavaDate(date.AddYears(1), out high, out low);
            }

            /** <inheritdoc /> */
            public DateTime FromJavaTicks(long high, int low)
            {
                var date = new DateTime(BinaryUtils.JavaDateTicks + high * TimeSpan.TicksPerMillisecond + low / 100,
                    DateTimeKind.Utc);

                return date.AddYears(1);
            }
        }
    }
}
