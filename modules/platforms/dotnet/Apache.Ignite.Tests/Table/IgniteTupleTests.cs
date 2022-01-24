/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Tests.Table
{
    using System;
    using System.Collections.Generic;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IIgniteTuple"/>.
    /// </summary>
    public class IgniteTupleTests
    {
        [Test]
        public void TestCreateUpdateRead()
        {
            IIgniteTuple tuple = new IgniteTuple();
            Assert.AreEqual(0, tuple.FieldCount);

            tuple["foo"] = 1;
            tuple["bar"] = Guid.Empty;

            Assert.AreEqual(2, tuple.FieldCount);

            Assert.AreEqual(1, tuple["foo"]);
            Assert.AreEqual(Guid.Empty, tuple["bar"]);

            Assert.AreEqual(1, tuple[0]);
            Assert.AreEqual(Guid.Empty, tuple[1]);

            Assert.AreEqual("FOO", tuple.GetName(0));
            Assert.AreEqual("BAR", tuple.GetName(1));

            Assert.AreEqual(0, tuple.GetOrdinal("foo"));
            Assert.AreEqual(0, tuple.GetOrdinal("Foo"));
            Assert.AreEqual(0, tuple.GetOrdinal("FOO"));
            Assert.AreEqual(0, tuple.GetOrdinal("\"FOO\""));
            Assert.AreEqual(-1, tuple.GetOrdinal("\"Foo\""));
            Assert.AreEqual(1, tuple.GetOrdinal("bar"));

            tuple[0] = 2;
            tuple["bar"] = "x";
            tuple["qux"] = null;

            Assert.AreEqual(3, tuple.FieldCount);

            Assert.AreEqual(2, tuple["foo"]);
            Assert.AreEqual("x", tuple[1]);
            Assert.IsNull(tuple[2]);
        }

        [Test]
        public void TestGetNullOrEmptyNameThrowsException()
        {
            var tuple = new IgniteTuple { ["Foo"] = 1 };

            var ex = Assert.Throws<IgniteClientException>(() => tuple.GetOrdinal(string.Empty));
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);

            ex = Assert.Throws<IgniteClientException>(() => tuple.GetOrdinal(null!));
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);

            ex = Assert.Throws<IgniteClientException>(() =>
            {
                var unused = tuple[string.Empty];
            });
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);

            ex = Assert.Throws<IgniteClientException>(() =>
            {
                var unused = tuple[null!];
            });
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);
        }

        [Test]
        public void TestGetNonExistingNameThrowsException()
        {
            var tuple = new IgniteTuple { ["Foo"] = 1 };

            var ex = Assert.Throws<KeyNotFoundException>(() =>
            {
                var unused = tuple["bar"];
            });
            Assert.AreEqual("The given key 'BAR' was not present in the dictionary.", ex!.Message);
        }

        [Test]
        public void TestToStringEmpty()
        {
            Assert.AreEqual("IgniteTuple []", new IgniteTuple().ToString());
        }

        [Test]
        public void TestToStringOneField()
        {
            var tuple = new IgniteTuple { ["foo"] = 1 };
            Assert.AreEqual("IgniteTuple [FOO=1]", tuple.ToString());
        }

        [Test]
        public void TestToStringTwoFields()
        {
            var tuple = new IgniteTuple
            {
                ["foo"] = 1,
                ["b"] = "abcd"
            };

            Assert.AreEqual("IgniteTuple [FOO=1, B=abcd]", tuple.ToString());
        }

        [Test]
        public void TestEquality()
        {
            var t1 = new IgniteTuple(2) { ["k"] = 1, ["v"] = "2" };
            var t2 = new IgniteTuple(3) { ["k"] = 1, ["v"] = "2" };
            var t3 = new IgniteTuple(4) { ["k"] = 1, ["v"] = null };

            Assert.AreEqual(t1, t2);
            Assert.AreEqual(t2, t1);
            Assert.AreEqual(t1.GetHashCode(), t2.GetHashCode());

            Assert.AreNotEqual(t1, t3);
            Assert.AreNotEqual(t1.GetHashCode(), t3.GetHashCode());

            Assert.AreNotEqual(t2, t3);
            Assert.AreNotEqual(t2.GetHashCode(), t3.GetHashCode());
        }

        [Test]
        public void TestCustomTupleEquality()
        {
            var tuple = new IgniteTuple { ["key"] = 42, ["val"] = "Val1" };
            var customTuple = new CustomTestIgniteTuple();

            Assert.IsTrue(IIgniteTuple.Equals(tuple, customTuple));
            Assert.AreEqual(IIgniteTuple.GetHashCode(tuple), IIgniteTuple.GetHashCode(customTuple));
        }
    }
}
