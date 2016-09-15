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

namespace Apache.Ignite.EntityFramework.Tests
{
    using System;
    using Apache.Ignite.EntityFramework.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ArrayDbDataReader"/>.
    /// </summary>
    public class ArrayDbDataReaderTests
    {
        /// <summary>
        /// Tests the reader.
        /// </summary>
        [Test]
        public void TestReader()
        {
            var data = new[]
            {
                new object[]
                {
                    (byte) 1, (short) 2, 3, (long) 4, (float) 5, (double) 6, (decimal) 7, "8", '9', DateTime.Now,
                    Guid.NewGuid(), false
                }
            };

            var schema = new []
            {
                new DataReaderField("fbyte", typeof(byte), "by"),
                new DataReaderField("fshort", typeof(short), "sh"),
                new DataReaderField("fint", typeof(int), "in"),
                new DataReaderField("flong", typeof(long), "lo"),
                new DataReaderField("ffloat", typeof(float), "fl"),
                new DataReaderField("fdouble", typeof(double), "do"),
                new DataReaderField("fdecimal", typeof(decimal), "de"),
                new DataReaderField("fstring", typeof(string), "st"),
                new DataReaderField("fchar", typeof(char), "ch"),
                new DataReaderField("fDateTime", typeof(DateTime), "Da"),
                new DataReaderField("fGuid", typeof(Guid), "Go"),
                new DataReaderField("fbool", typeof(bool), "bo"),
            };

            // Create reader,
            var reader = new ArrayDbDataReader(data, schema);

            // Check basic props.
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(0, reader.Depth);
            Assert.AreEqual(0, reader.RecordsAffected);
            Assert.AreEqual(12, reader.FieldCount);
            Assert.AreEqual(12, reader.VisibleFieldCount);
            Assert.IsFalse(reader.IsClosed);
            Assert.IsTrue(reader.HasRows);

            // Check reading.
            Assert.AreEqual(1, reader.GetByte(reader.GetOrdinal("fbyte")));
            Assert.AreEqual("by", reader.GetDataTypeName(0));
            Assert.AreEqual(typeof(byte), reader.GetFieldType(0));

            Assert.AreEqual(2, reader.GetInt16(reader.GetOrdinal("fshort")));
            Assert.AreEqual("sh", reader.GetDataTypeName(1));
            Assert.AreEqual(typeof(short), reader.GetFieldType(1));

            Assert.AreEqual(3, reader.GetInt32(reader.GetOrdinal("fint")));
            Assert.AreEqual("in", reader.GetDataTypeName(2));
            Assert.AreEqual(typeof(int), reader.GetFieldType(2));

            Assert.AreEqual(4, reader.GetInt64(reader.GetOrdinal("flong")));
            Assert.AreEqual("lo", reader.GetDataTypeName(3));
            Assert.AreEqual(typeof(long), reader.GetFieldType(3));

            Assert.AreEqual(5, reader.GetFloat(reader.GetOrdinal("ffloat")));
            Assert.AreEqual("fl", reader.GetDataTypeName(4));
            Assert.AreEqual(typeof(float), reader.GetFieldType(4));

            // Close.
            reader.Close();
            Assert.IsTrue(reader.IsClosed);
        }
    }
}
