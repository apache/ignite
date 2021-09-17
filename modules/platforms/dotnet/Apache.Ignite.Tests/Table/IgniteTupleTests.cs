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

            Assert.AreEqual("foo", tuple.GetName(0));
            Assert.AreEqual("bar", tuple.GetName(1));

            Assert.AreEqual(0, tuple.GetOrdinal("foo"));
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
        public void TestToStringEmpty()
        {
            Assert.AreEqual("IgniteTuple []", new IgniteTuple().ToString());
        }

        [Test]
        public void TestToStringOneField()
        {
            var tuple = new IgniteTuple { ["foo"] = 1 };
            Assert.AreEqual("IgniteTuple [foo=1]", tuple.ToString());
        }

        [Test]
        public void TestToStringTwoFields()
        {
            var tuple = new IgniteTuple
            {
                ["foo"] = 1,
                ["b"] = "abcd"
            };

            Assert.AreEqual("IgniteTuple [foo=1, b=abcd]", tuple.ToString());
        }
    }
}
