/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// DateTime binary serialization tests.
    /// </summary>
    public class BinaryDateTimeTest
    {
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
            Func<T, DateTime> getValue, string fieldName) where T : new()
        {
            // Non-UTC DateTime throws.
            var binary = Ignition.GetIgnite().GetBinary();

            var obj = new T();

            setValue(obj, DateTime.Now);

            var ex = Assert.Throws<BinaryObjectException>(() => binary.ToBinary<IBinaryObject>(obj), 
                "Timestamp fields should throw an error on non-UTC values");

            Assert.AreEqual("DateTime is not UTC. Only UTC DateTime can be used for interop with other platforms.",
                ex.Message);

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
    }
}
