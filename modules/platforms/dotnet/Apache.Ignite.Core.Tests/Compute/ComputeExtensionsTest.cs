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

// ReSharper disable AccessToModifiedClosure
namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Tests;
    using NUnit.Framework;

    /// <summary>
    /// ComputeExtensions tests.
    /// </summary>
    [Serializable]
    public class ComputeExtensionsTest : IgniteTestBase
    {
        /** */
        private string _testCapturedString = "testFieldValue";

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeExtensionsTest"/> class.
        /// </summary>
        public ComputeExtensionsTest()
            : base("config\\compute\\compute-grid1.xml", "config\\compute\\compute-grid2.xml")
        {
        }

        /// <summary>
        /// Tests Call methods.
        /// </summary>
        [Test]
        public void TestCall()
        {
            // Test static
            Assert.AreEqual(15, Compute.Call(() => 15));

            CollectionAssert.AreEquivalent(new[] {1, 2}, Compute.Call(new Func<int>[] {() => 1, () => 2}));

            Assert.AreEqual(3, Compute.Call(new Func<int>[] {() => 1, () => 2}, 
                x => x.Aggregate((x1, x2) => x1 + x2)));

            // Test captured variable
            object testVal = "test";

            Assert.AreEqual(testVal, Compute.Call(() => testVal));

            CollectionAssert.AreEquivalent(new[] {testVal}, Compute.Call(new Func<object>[] {() => testVal}));

            Assert.AreEqual(testVal, Compute.Call(new Func<object>[] {() => testVal}, x => x.Single()));

            // Test captured field
            Assert.AreEqual(_testCapturedString, Compute.Call(() => _testCapturedString));

            CollectionAssert.AreEquivalent(new[] {_testCapturedString},
                Compute.Call(new Func<object>[] {() => _testCapturedString}));

            Assert.AreEqual(_testCapturedString,
                Compute.Call(new Func<object>[] {() => _testCapturedString}, x => x.Single()));

            // Test unsupported types
            testVal = new NonSerializable();

            Assert.Throws<SerializationException>(() => Compute.Call(() => testVal));
        }

        /// <summary>
        /// Tests Call methods.
        /// </summary>
        [Test]
        public void TestAffinityCall()
        {
            // Test captured variable
            object testVal = "test";

            var result = Compute.AffinityCall(null, 1, () => testVal);

            Assert.AreEqual(testVal, result);

            // Test captured field
            result = Compute.AffinityCall(null, 1, () => _testCapturedString);

            Assert.AreEqual(_testCapturedString, result);

            // Test unsupported types
            testVal = new NonSerializable();

            Assert.Throws<SerializationException>(() => Compute.AffinityCall(null, 1, () => testVal));
        }

        /// <summary>
        /// Tests Apply methods.
        /// </summary>
        [Test]
        public void TestApply()
        {
            // Test static
            Assert.AreEqual(2, Compute.Apply(x => x + 1, 1));

            CollectionAssert.AreEquivalent(new[] { "1", "2", "3" },
                Compute.Apply<int, string>(x => x.ToString(), new[] {1, 2, 3}));

            Assert.AreEqual(5, Compute.Apply(x => x + 3, new[] {2}, x => x.Single()));

            // Test captured variable
            var testVal = "test";

            Assert.AreEqual("test1", Compute.Apply(x => testVal + x, 1));

            var args = Enumerable.Range(1, 10).ToArray();

            CollectionAssert.AreEquivalent(args.Select(x => testVal + x),
                Compute.Apply<int, string>(x => testVal + x, args));

            Assert.AreEqual("test1", Compute.Apply(x => testVal + x, new[] { 1 }, x => x.Single()));

            // Test captured field
            Assert.AreEqual(_testCapturedString + 1, Compute.Apply(x => _testCapturedString + x, 1));

            CollectionAssert.AreEquivalent(args.Select(x => _testCapturedString + x),
                Compute.Apply<int, string>(x => _testCapturedString + x, args));

            Assert.AreEqual(_testCapturedString + 1,
                Compute.Apply(x => _testCapturedString + x, new[] {1}, x => x.Single()));

            // Test unsupported types
            var testInvalid = new NonSerializable();

            Assert.Throws<BinaryObjectException>(() => Compute.Apply(x => x.ToString(), testInvalid));

            Assert.Throws<BinaryObjectException>(
                () => Compute.Apply<object, string>(x => x.ToString(), new[] {testInvalid}));

            Assert.Throws<SerializationException>(
                () => Compute.Apply(x => testInvalid, new[] { 1 }, x => x.Single()));
        }

        /// <summary>
        /// Tests Run methods.
        /// </summary>
        [Test]
        public void TestRun()
        {
            // Local run affects captured variable
            var x = 1;

            Grid.GetCluster().ForLocal().GetCompute().Run(() => x++);
            Assert.AreEqual(2, x);

            Grid.GetCluster().ForLocal().GetCompute().Run(new Action[] {() => x++, () => x += 2});
            Assert.AreEqual(5, x);

            // Remote run does not
            Grid.GetCluster().ForRemotes().GetCompute().Run(() => x++);
            Assert.AreEqual(5, x);

            Grid.GetCluster().ForRemotes().GetCompute().Run(new Action[] { () => x++, () => x += 2 });
            Assert.AreEqual(5, x);

            // Test captured field
            var expected = _testCapturedString + "1";

            Grid.GetCluster().ForLocal().GetCompute().Run(() => _testCapturedString += "1");

            Assert.AreEqual(expected, _testCapturedString);

            // Test unsupported type
            var testInvalid = new NonSerializable();

            Assert.Throws<SerializationException>(() => Compute.Run(() => testInvalid.Prop = 10));
        }

        /// <summary>
        /// Tests Run methods.
        /// </summary>
        [Test]
        public void TestAffinityRun()
        {
            for (var i = 0; i < 100; i++)
            {
                var node = Grid.GetAffinity(null).MapKeyToNode(i);
                
                var x = 1;
                
                var isLocal = node.Id == Grid.GetCluster().GetLocalNode().Id;
                
                Compute.AffinityRun(null, i, () => x++);

                // Local run affects captured variable, remote run does not
                Assert.AreEqual(isLocal ? 2 : 1, x);

                if (isLocal)
                {
                    var expected = _testCapturedString + "1";
                    
                    Compute.AffinityRun(null, i, () => _testCapturedString += "1");

                    Assert.AreEqual(expected, _testCapturedString);
                }

                // Test unsupported type
                var testInvalid = new NonSerializable();

                Assert.Throws<SerializationException>(() => Compute.AffinityRun(null, i, () => testInvalid.Prop = 10));
            }
        }

        /// <summary>
        /// Tests Broadcast methods.
        /// </summary>
        [Test]
        public void TestBroadcastAction()
        {
            // variable capture
            var x = 1;

            Compute.Broadcast(() => { x++; });

            Assert.AreEqual(2, x);

            // field capture
            var expected = _testCapturedString + "1";

            Compute.Broadcast(() => _testCapturedString += "1");

            Assert.AreEqual(expected, _testCapturedString);

            // unsupported type
            Assert.Throws<SerializationException>(() =>
            {
                var testInvalid = new NonSerializable();

                Compute.Broadcast(() => { testInvalid.Prop = 10; });
            });
        }

        /// <summary>
        /// Tests Broadcast methods.
        /// </summary>
        [Test]
        public void TestBroadcastFunc()
        {
            // static
            Assert.AreEqual(new[] {5, 5}, Compute.Broadcast(() => 5));

            // variable capture
            var x = 10;

            Assert.AreEqual(new[] { 10, 10 }, Compute.Broadcast(() => x));

            // field capture
            Assert.AreEqual(new[] {_testCapturedString, _testCapturedString}, 
                Compute.Broadcast(() => _testCapturedString));

            // unsupported type
            Assert.Throws<SerializationException>(() =>
            {
                var testInvalid = new NonSerializable();

                Compute.Broadcast(() => testInvalid.Prop);
            });
        }

        /// <summary>
        /// Tests Broadcast methods.
        /// </summary>
        [Test]
        public void TestBroadcastFuncArg()
        {
            // static
            Assert.AreEqual(new[] {5, 5}, Compute.Broadcast(a => a, 5));

            // variable capture
            var x = 10;

            Assert.AreEqual(new[] { 13, 13 }, Compute.Broadcast(a => x + a, 3));

            // field capture
            Assert.AreEqual(new[] {_testCapturedString + "5", _testCapturedString + "5"}, 
                Compute.Broadcast(a => _testCapturedString + a, "5"));

            // unsupported type
            Assert.Throws<SerializationException>(() =>
            {
                var testInvalid = new NonSerializable();

                Compute.Broadcast(a => testInvalid.Prop, 0);
            });
        }

        /// <summary>
        /// Non-serializable func.
        /// </summary>
        private class NonSerializable
        {
            public int Prop { get; set; }
        }
    }
}