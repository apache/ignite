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
    using System.Linq;
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
            var dateTime = DateTime.Now;
            var guid = Guid.NewGuid();

            var data = new[]
            {
                new object[]
                {
                    (byte) 1, (short) 2, 3, (long) 4, (float) 5, (double) 6, (decimal) 7, "8", '9', dateTime,
                    guid, false, new byte[] {1,2}, new[] {'a','b'}
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
                new DataReaderField("fGuid", typeof(Guid), "Gu"),
                new DataReaderField("fbool", typeof(bool), "bo"),
                new DataReaderField("fbytes", typeof(byte[]), "bb"),
                new DataReaderField("fchars", typeof(char[]), "cc"),
            };

            // Create reader,
            var reader = new ArrayDbDataReader(data, schema);

            // Check basic props.
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(0, reader.Depth);
            Assert.AreEqual(-1, reader.RecordsAffected);
            Assert.AreEqual(14, reader.FieldCount);
            Assert.AreEqual(14, reader.VisibleFieldCount);
            Assert.IsFalse(reader.IsClosed);
            Assert.IsTrue(reader.HasRows);

            // Check reading.
            var data2 = new object[14];
            Assert.AreEqual(14, reader.GetValues(data2));
            Assert.AreEqual(data[0], data2);

            Assert.AreEqual(1, reader.GetByte(reader.GetOrdinal("fbyte")));
            Assert.AreEqual("by", reader.GetDataTypeName(0));
            Assert.AreEqual(typeof(byte), reader.GetFieldType(0));
            Assert.AreEqual("fbyte", reader.GetName(0));
            Assert.AreEqual(1, reader["fbyte"]);
            Assert.AreEqual(1, reader[0]);

            Assert.AreEqual(2, reader.GetInt16(reader.GetOrdinal("fshort")));
            Assert.AreEqual("sh", reader.GetDataTypeName(1));
            Assert.AreEqual(typeof(short), reader.GetFieldType(1));
            Assert.AreEqual("fshort", reader.GetName(1));
            Assert.AreEqual(2, reader["fshort"]);
            Assert.AreEqual(2, reader[1]);

            Assert.AreEqual(3, reader.GetInt32(reader.GetOrdinal("fint")));
            Assert.AreEqual("in", reader.GetDataTypeName(2));
            Assert.AreEqual(typeof(int), reader.GetFieldType(2));
            Assert.AreEqual("fint", reader.GetName(2));
            Assert.AreEqual(3, reader["fint"]);
            Assert.AreEqual(3, reader[2]);

            Assert.AreEqual(4, reader.GetInt64(reader.GetOrdinal("flong")));
            Assert.AreEqual("lo", reader.GetDataTypeName(3));
            Assert.AreEqual(typeof(long), reader.GetFieldType(3));
            Assert.AreEqual("flong", reader.GetName(3));
            Assert.AreEqual(4, reader["flong"]);
            Assert.AreEqual(4, reader[3]);

            Assert.AreEqual(5, reader.GetFloat(reader.GetOrdinal("ffloat")));
            Assert.AreEqual("fl", reader.GetDataTypeName(4));
            Assert.AreEqual(typeof(float), reader.GetFieldType(4));
            Assert.AreEqual("ffloat", reader.GetName(4));
            Assert.AreEqual(5, reader["ffloat"]);
            Assert.AreEqual(5, reader[4]);

            Assert.AreEqual(6, reader.GetDouble(reader.GetOrdinal("fdouble")));
            Assert.AreEqual("do", reader.GetDataTypeName(5));
            Assert.AreEqual(typeof(double), reader.GetFieldType(5));
            Assert.AreEqual("fdouble", reader.GetName(5));
            Assert.AreEqual(6, reader["fdouble"]);
            Assert.AreEqual(6, reader[5]);

            Assert.AreEqual(7, reader.GetDecimal(reader.GetOrdinal("fdecimal")));
            Assert.AreEqual("de", reader.GetDataTypeName(6));
            Assert.AreEqual(typeof(decimal), reader.GetFieldType(6));
            Assert.AreEqual("fdecimal", reader.GetName(6));
            Assert.AreEqual(7, reader["fdecimal"]);
            Assert.AreEqual(7, reader[6]);

            Assert.AreEqual("8", reader.GetString(reader.GetOrdinal("fstring")));
            Assert.AreEqual("st", reader.GetDataTypeName(7));
            Assert.AreEqual(typeof(string), reader.GetFieldType(7));
            Assert.AreEqual("fstring", reader.GetName(7));
            Assert.AreEqual("8", reader["fstring"]);
            Assert.AreEqual("8", reader[7]);

            Assert.AreEqual('9', reader.GetChar(reader.GetOrdinal("fchar")));
            Assert.AreEqual("ch", reader.GetDataTypeName(8));
            Assert.AreEqual(typeof(char), reader.GetFieldType(8));
            Assert.AreEqual("fchar", reader.GetName(8));
            Assert.AreEqual('9', reader["fchar"]);
            Assert.AreEqual('9', reader[8]);

            Assert.AreEqual(dateTime, reader.GetDateTime(reader.GetOrdinal("fDateTime")));
            Assert.AreEqual("Da", reader.GetDataTypeName(9));
            Assert.AreEqual(typeof(DateTime), reader.GetFieldType(9));
            Assert.AreEqual("fDateTime", reader.GetName(9));
            Assert.AreEqual(dateTime, reader["fDateTime"]);
            Assert.AreEqual(dateTime, reader[9]);

            Assert.AreEqual(guid, reader.GetGuid(reader.GetOrdinal("fGuid")));
            Assert.AreEqual("Gu", reader.GetDataTypeName(10));
            Assert.AreEqual(typeof(Guid), reader.GetFieldType(10));
            Assert.AreEqual("fGuid", reader.GetName(10));
            Assert.AreEqual(guid, reader["fGuid"]);
            Assert.AreEqual(guid, reader[10]);

            Assert.AreEqual(false, reader.GetBoolean(reader.GetOrdinal("fbool")));
            Assert.AreEqual("bo", reader.GetDataTypeName(11));
            Assert.AreEqual(typeof(bool), reader.GetFieldType(11));
            Assert.AreEqual("fbool", reader.GetName(11));
            Assert.AreEqual(false, reader["fbool"]);
            Assert.AreEqual(false, reader[11]);

            var bytes = new byte[2];
            Assert.AreEqual(2, reader.GetBytes(reader.GetOrdinal("fbytes"),0, bytes, 0, 2));
            Assert.AreEqual(data[0][12], bytes);
            Assert.AreEqual("bb", reader.GetDataTypeName(12));
            Assert.AreEqual(typeof(byte[]), reader.GetFieldType(12));
            Assert.AreEqual("fbytes", reader.GetName(12));
            Assert.AreEqual(data[0][12], reader["fbytes"]);
            Assert.AreEqual(data[0][12], reader[12]);

            var chars = new char[2];
            Assert.AreEqual(2, reader.GetChars(reader.GetOrdinal("fchars"),0, chars, 0, 2));
            Assert.AreEqual(data[0][13], chars);
            Assert.AreEqual("cc", reader.GetDataTypeName(13));
            Assert.AreEqual(typeof(char[]), reader.GetFieldType(13));
            Assert.AreEqual("fchars", reader.GetName(13));
            Assert.AreEqual(data[0][13], reader["fchars"]);
            Assert.AreEqual(data[0][13], reader[13]);

            Assert.IsFalse(Enumerable.Range(0, 14).Any(x => reader.IsDBNull(x)));

            // Close.
            reader.Close();
            Assert.IsTrue(reader.IsClosed);
        }
    }
}
