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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System;
    using System.Reflection;
    using NUnit.Framework;

    /// <summary>
    /// Tests delegate serialization.
    /// </summary>
    public class DelegatesTest
    {
        /** Test int value. */
        private static int _int;

        /** Test delegate. */
        private delegate string LowerSubStringDelegate(string s, int startIndex);

        /// <summary>
        /// Tests that delegates can be serialized.
        /// </summary>
        [Test]
        public void TestAction()
        {
            // Action with captured variable.
            var val = new PrimitivesTest.Primitives {Int = 135};

            Action act = () => {
                val.Int++;
                _int = val.Int;
            };

            var res = TestUtils.SerializeDeserialize(act);
            Assert.AreEqual(act.Method, res.Method);
            Assert.AreNotEqual(act.Target, res.Target);

            res();
            Assert.AreEqual(135, val.Int);   // Captured variable is deserialized to a new instance.
            Assert.AreEqual(136, _int);

            // Action with arguments.
            Action<PrimitivesTest.Primitives, int> act1 = (p, i) => { p.Int = i; };

            var res1 = TestUtils.SerializeDeserialize(act1);
            Assert.AreEqual(act1.Method, res1.Method);

            res1(val, 33);
            Assert.AreEqual(33, val.Int);
        }

        /// <summary>
        /// Tests that anonymous function can be serialized.
        /// </summary>
        [Test]
        public void TestFunc()
        {
            int ms = DateTime.Now.Millisecond;

            Func<int, int> func = x => x + ms;

            var resFunc = TestUtils.SerializeDeserialize(func);
            Assert.AreEqual(func.Method, resFunc.Method);
            Assert.AreNotEqual(func.Target, resFunc.Target);

            Assert.AreEqual(ms + 20, resFunc(20));
        }

        /// <summary>
        /// Tests that old-fashioned delegate can be serialized.
        /// </summary>
        [Test]
        public void TestDelegate()
        {
            // Delegate to a static method.
            LowerSubStringDelegate del1 = LowerSubString;

            var del1Res = TestUtils.SerializeDeserialize(del1);

            Assert.AreEqual(del1.Method, del1Res.Method);
            Assert.IsNull(del1Res.Target);

            Assert.AreEqual("ooz", del1Res("FOOZ", 1));

            // Delegate to an anonymous method.
            LowerSubStringDelegate del2 = (s, i) => s.Substring(i).ToLower();

            var del2Res = TestUtils.SerializeDeserialize(del2);

            Assert.AreEqual(del2.Method, del2Res.Method, "Delegate methods are same");

            Assert.AreEqual("ooz", del2Res("FOOZ", 1), "Delegate works as expected");
        }

        /// <summary>
        /// Tests that MethodInfo can be serialized.
        /// </summary>
        [Test]
        public void TestMethodInfo()
        {
            var methods = typeof(string).GetMethods(BindingFlags.Public | BindingFlags.NonPublic
                                                    | BindingFlags.Instance | BindingFlags.Static);

            Assert.IsNotEmpty(methods);

            foreach (var methodInfo in methods)
            {
                var res = TestUtils.SerializeDeserialize(methodInfo);

                Assert.AreEqual(methodInfo.Name, res.Name);
                Assert.AreEqual(methodInfo.DeclaringType, res.DeclaringType);
                Assert.AreEqual(methodInfo.ReturnType, res.ReturnType);
                Assert.AreEqual(methodInfo.GetParameters(), res.GetParameters());
            }
        }

        /// <summary>
        /// Tests that recursive anonymous function can be serialized.
        /// </summary>
        [Test]
        public void TestRecursiveFunc()
        {
            Func<int, int> fib = null;
            fib = x => x == 0
                ? 0
                : x == 1
                    ? 1
                    : fib(x - 2) + fib(x - 1);

            Assert.AreEqual(89, fib(11));
            Assert.AreEqual(144, fib(12));

            var resFib = TestUtils.SerializeDeserialize(fib);

            Assert.AreEqual(fib.Method, resFib.Method);

            Assert.AreEqual(89, resFib(11));
            Assert.AreEqual(144, resFib(12));
        }

        private static string LowerSubString(string s, int startIndex)
        {
            return s.Substring(startIndex).ToLower();
        }
    }
}
