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
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests binary type interoperability between .NET and Java code.
    /// </summary>
    public class JavaBinaryInteropTest
    {
        /// <summary>
        /// Tests that all kinds of values from .NET can be handled properly on Java side.
        /// </summary>
        [Test]
        public void TestValueRoundtrip()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                ignite.CreateCache<int, object>("default");

                // Basic types.
                // Types which map directly to Java are returned properly when retrieved as object.
                // Non-directly mapped types are returned as their counterpart.
                CheckValueCaching((char) 128);
                CheckValueCaching((byte) 255);
                CheckValueCaching((sbyte) -10, false);
                CheckValueCaching((short) -32000);
                CheckValueCaching((ushort) 65350, false);
                CheckValueCaching(int.MinValue);
                CheckValueCaching(uint.MaxValue, false);
                CheckValueCaching(long.MinValue);
                CheckValueCaching(ulong.MaxValue, false);

                CheckValueCaching((float) 1.1);
                CheckValueCaching(2.2);

                CheckValueCaching((decimal) 3.3, asArray: false);
                CheckValueCaching(Guid.NewGuid(), asArray: false);
                CheckValueCaching(DateTime.Now, asArray: false);

                CheckValueCaching("foobar");

                // Special arrays.
                CheckValueCaching(new[] {Guid.Empty, Guid.NewGuid()}, false);
                CheckValueCaching(new Guid?[] {Guid.Empty, Guid.NewGuid()});

                CheckValueCaching(new[] {1.2m, -3.4m}, false);
                CheckValueCaching(new decimal?[] {1.2m, -3.4m});

                CheckValueCaching(new[] {DateTime.Now}, false);

                // Custom types.
                CheckValueCaching(new Foo {X = 10}, asArray: false);
                CheckValueCaching(new Bar {X = 20}, asArray: false);

                // Collections.
                CheckValueCaching(new List<Foo>(GetFoo()));
                CheckValueCaching(new List<Bar>(GetBar()));

                CheckValueCaching(new HashSet<Foo>(GetFoo()));
                CheckValueCaching(new HashSet<Bar>(GetBar()));

                CheckValueCaching(GetFoo().ToDictionary(x => x.X, x => x));
                CheckValueCaching(GetBar().ToDictionary(x => x.X, x => x));

                // Custom type arrays.
                // Array type is lost, because in binary mode on Java side we receive the value as Object[].
                CheckValueCaching(new[] {new Foo {X = -1}, new Foo {X = 1}}, false);
                CheckValueCaching(new[] {new Bar {X = -10}, new Bar {X = 10}}, false);
            }
        }

        /// <summary>
        /// Checks caching of a value with generic cache.
        /// </summary>
        private static void CheckValueCaching<T>(T val, bool asObject = true, bool asArray = true)
        {
            var cache = Ignition.GetIgnite().GetCache<int, T>("default");

            cache[1] = val;
            Assert.AreEqual(val, cache[1]);

            if (asObject)
            {
                CheckValueCachingAsObject(val);
            }

            // Array of T
            if (asArray && !(val is IEnumerable))
            {
                CheckValueCaching(new[] {val}, asObject, false);
            }
        }

        /// <summary>
        /// Checks caching of a value with object cache.
        /// </summary>
        private static void CheckValueCachingAsObject<T>(T val)
        {
            var cache = Ignition.GetIgnite().GetCache<int, object>("default");

            cache[1] = val;
            Assert.AreEqual(val, (T) cache[1]);
        }

        /// <summary>
        /// Gets Foo collection.
        /// </summary>
        private static IEnumerable<Foo> GetFoo()
        {
            return Enumerable.Range(-50, 100).Select(x => new Foo {X = x});
        }

        /// <summary>
        /// Gets Bar collection.
        /// </summary>
        private static IEnumerable<Bar> GetBar()
        {
            return Enumerable.Range(-50, 100).Select(x => new Bar {X = x});
        }

        /// <summary>
        /// Test custom class.
        /// </summary>
        private class Foo
        {
            public int X { get; set; }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return X == ((Foo) obj).X;
            }

            public override int GetHashCode()
            {
                return X;
            }
        }

        /// <summary>
        /// Test custom struct.
        /// </summary>
        private struct Bar
        {
            public int X { get; set; }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is Bar && X == ((Bar) obj).X;
            }

            public override int GetHashCode()
            {
                return X;
            }
        }
    }
}
